import logging
import math
import random
import time
from datetime import timedelta, datetime, date

from typing import Generator, Iterable, Tuple, Dict, Callable, Union

from common.enums.entity import Entity
from common.enums.jobtype import JobType, detect_job_type
from common.enums.reporttype import ReportType
from common.error_inspector import ErrorInspector
from common.measurement import Measure
from common.tztools import now, now_in_tz, dt_to_other_timezone
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.errors import ScoringException
from sweep_builder.account_cache import AccountCache

logger = logging.getLogger(__name__)

MAX_SCORE_MULTIPLIER = 1.0
MIN_SCORE_MULTIPLIER = 0.5
JOB_MIN_SUCCESS_PERIOD_IN_DAYS = 30
JOB_MAX_AGE_IN_DAYS = 365 * 2
MUST_RUN_SCORE = 1000

INFINITY = float('inf')
DAY_IN_SECONDS = 60*60*24

AdTree = [
    Entity.AdAccount,
    Entity.Campaign,
    Entity.AdSet,
    Entity.Ad,
    Entity.AdCreative,
    Entity.AdVideo,
    Entity.CustomAudience,
]
AdTree_len = len(AdTree)

PageTree = [
    Entity.Page,
    Entity.PageVideo,
    Entity.PagePost,
]
PageTree_len = len(PageTree)

ReportingDayTypePriority = {
    ReportType.day: 1.0,
    ReportType.day_age_gender: 0.9,
    ReportType.day_dma: 0.9,
    ReportType.day_region: 0.9,
    ReportType.day_country: 0.8,
    ReportType.day_hour: 0.7,
    ReportType.day_platform: 0.7,
}

class ScoreSkewHandlers:
    # nested as such mostly for ease of mocking in tests.
    # do NOT unbundle this class.

    @staticmethod
    def get_now():
        # factored out to allow mocking in tests
        return datetime.now()

    @staticmethod
    def same_score(claim: ScorableClaim) -> float:
        return MAX_SCORE_MULTIPLIER

    @classmethod
    def lifetime_skew(cls, claim: ScorableClaim) -> float:
        # focus is on collecting most-recent data
        # but we gravitate towards "top of the hour" timeslots
        # The closer to "top of hour" the higher the score
        # (We used to make lifetime diff tables to capture uniques better
        #  but because of throttling and timing, it's very hard to guarantee "every hour on hour")
        # This also helps rotate scores between lifetime and non-lifetime metrics within the hour.
        m = cls.get_now().minute
        # movement from 0+ to 30 minutes and backward movement from 59- to 30
        # produce scores from 1.0 to 0.0 (with typical floating point errors)
        return abs(30 - m) / 30

    @classmethod
    def organic_lifetime_skew(cls, claim: ScorableClaim) -> float:
        m = cls.get_now().minute
        # opposite of normal (paid) lifetime
        return 1 - abs(30 - m) / 30

    @staticmethod
    def reporting_day_skew(claim: ScorableClaim) -> float:
        # contemplate rotating these around the hour like lifetime,
        # or rather skew them into slots not naturally occupied by lifetime jobs
        i = ReportingDayTypePriority.get(claim.report_type, random.randrange(50, 90) / 100.0)

    @staticmethod
    def entity_hierarchy_skew(claim: ScorableClaim) -> float:
        try:
            i = AdTree.index(claim.report_variant)
            # upper ~60% of range + random
            return (1.0 - i/(AdTree_len*1.6)) * random.randrange(50, 200) / 100.0
        except ValueError:
            try:
                i = PageTree.index(claim.report_variant)
                # lower 60% of range + random
                return (0.6 * (1.0 - i/PageTree_len)) * random.randrange(50, 150) / 100.0
            except ValueError:
                return random.randrange(50, 80) / 100.0

    # 1-3 days from zero
    # /^\
    #    \
    #     \
    #      \ | ~10 days after zero, ~8 days after apogee
    # ------\-----------> Reporting Day days from >now<
    report_day_from_now_curve = (
        # end of range inclusive
        #   curve function for the range of start / end
        (1, lambda d: 0.75 + d * 0.25),  # up
        (3, lambda d: 1.0),  # flat
        (INFINITY, lambda d: 1.0 - d * 0.128),  # down. 0.128 = 1/8ths down per day from 1 to 0
    )

    # First day we don't care much
    # /-------\
    # |        \-----------  <- ~90% (something is wrong here... let go a little)
    #-'
    #
    # -----------------> Reporting Day days from >now<
    report_day_from_now_no_success_curve = (
        # end of range inclusive
        #   curve function for the range of start / end
        (1, lambda d: 0.75),  # flat
        (10, lambda d: 1.0),  # flat
        (INFINITY, lambda d: 0.9),  # flat
    )

    #
    #
    #      10
    #  _..--^-------......_______ <- Peak at ~20% of value down to zero in some 2 years.
    # --------------------------> Reporting Day Days away from from >last success<
    report_day_from_last_success_curve = (
        # end of range inclusive
        #   curve function for the range of start / end
        (20, lambda d: 0.0 + d * 0.01),  # up from 0.0 to 0.2 over 20 days
        # here score jumps from 0.128 to 0.20 :)
        (INFINITY, lambda d: 0.2 - d * 0.0005),  # down. -0.0005 = down per day from 0.20 to 0 over a year
    )

    # First day we don't care much
    # /-------\
    # |        \-----------  <- ~90% (something is something chronically wrong here... let go easy a little)
    # '
    # /
    # -----------------> last success from >now<
    lifetime_curve = (
        # end of range inclusive
        #   curve function for the range of start / end
        (1, lambda d: 0.0 + d * 0.6),
        (11, lambda d: 0.6 + d * 0.06), # up to 1.0
        (INFINITY, lambda d: 0.9),  # flat
    )

    #    ~2 day
    #    /-------------
    #   /
    #  /
    # /
    # -----------------> last success from >now<
    # Note that Entity tree has special level-based skew that knocks individual entity_types down.
    entity_curve = (
        # end of range inclusive
        #   curve function for the range of start / end
        (2, lambda d: d * 0.5),
        (INFINITY, lambda d: 1.0),  # flat
    )

    DEFAULT_TIMEZONE = 'America/Los_Angeles'

    @staticmethod
    def pick_curve(value, curves) -> Callable[[float], float]:
        for range_end, fn in curves:
            if value < range_end:
                return fn

        return lambda d: 1.0

    @classmethod
    def history_ratio_entities(cls, claim: ScorableClaim) -> float:
        last_success_dt = claim.last_report.last_success_dt if claim.last_report else None

        if not last_success_dt:
            return MAX_SCORE_MULTIPLIER

        _now = datetime.utcnow()
        days = (_now - last_success_dt.replace(tzinfo=None)).total_seconds() / DAY_IN_SECONDS
        return cls.pick_curve(days, cls.entity_curve)(days)

    @classmethod
    def history_ratio_lifetime(cls, claim: ScorableClaim) -> float:
        last_success_dt = claim.last_report.last_success_dt if claim.last_report else None

        if not last_success_dt:
            return MAX_SCORE_MULTIPLIER

        _now = datetime.utcnow()
        days = (_now - last_success_dt.replace(tzinfo=None)).total_seconds() / DAY_IN_SECONDS
        return cls.pick_curve(days, cls.lifetime_curve)(days)

    @classmethod
    def history_ratio_day_reports(cls, claim: ScorableClaim) -> float:
        """
        Reporting-Date-based records are super special.
        Because they are separate per each day, once collected with "success"
        you could be very wrong NOT to collect it again a day later.

        (all below points below assume you have prior collection success. No success == die trying)

        Thoughts:
        - collection worker day == record reporting day:
            collect OPPORTUNISTICALLY several time within the day
            Some reporting users watch "today" each hour so, skipping micro-updates
            on "todays" reports is NO-NO.
            But you also don't need to bust your ass trying to keep it super-fresh.
            Stay "mid-high" in scores.
        - Collection worker day == a day or two immediately after reporting day
            Super high importance to refresh number at least daily.
            You need to "fix" in these days "partial" numbers you grabbed when you did
            intra-day collection.
            You need to "fix" the "first 2-3 days numbers comingg from platform are under" issue.
        - Collection worker day = more than a week after Reporting Day
            Refresh these records with low-mid scores. Everything else can come in-front
        - if it's detected that the historical collection date for a
            record is too close to the reporting day, we boost up the score.
        """

        reporting_dt : Union[date,datetime] = claim.range_start
        if not reporting_dt:
            raise ValueError(f"Job {claim.job_id} cannot be scored. Day-based reports must have starting date.")
        # approximation to "some last timezone in the world" to America/Los_Angeles is good
        # it's the most conservative "there is still 'yesterday' somewhere in the world" timezone
        # (without counting tzs west of LA as "somewhere")

        tz = claim.timezone or cls.DEFAULT_TIMEZONE
        _now = now_in_tz(tz).replace(tzinfo=None)

        # datetime is subinstance of date. date is NOT subinstance of datetime
        # add 00:00:00 to date to make it dt
        reporting_dt = reporting_dt if isinstance(reporting_dt, datetime) else datetime.combine(reporting_dt, datetime.min.time())
        days_from_now_to_reporting_date = (_now - reporting_dt).total_seconds() / DAY_IN_SECONDS

        last_success_dt = claim.last_report.last_success_dt if claim.last_report else None

        if not last_success_dt:
            # there is only one chart we use
            return cls.pick_curve(days_from_now_to_reporting_date, cls.report_day_from_now_no_success_curve)(
                days_from_now_to_reporting_date
            )

        # from here down, we have prior last_success_dt

        last_success_dt_local = dt_to_other_timezone(last_success_dt, tz).replace(tzinfo=None)
        days_from_last_success_to_reporting_date = (last_success_dt_local - reporting_dt).total_seconds() / DAY_IN_SECONDS

        from_now_rate = cls.pick_curve(
            days_from_now_to_reporting_date,
            cls.report_day_from_now_curve
        )(
            days_from_now_to_reporting_date
        )

        from_last_success_rate = cls.pick_curve(
            days_from_last_success_to_reporting_date,
            cls.report_day_from_last_success_curve
        )(
            days_from_last_success_to_reporting_date
        )

        return max(
            from_now_rate,
            from_last_success_rate
        )


# You don't have to list all possible report types here.
# same_score is default if not on this list,
# but it helps to list possibilities for our record
SCORE_SKEW_HANDLERS: Dict[Tuple[str, str], Callable[[ScorableClaim], float]] = {
    (JobType.PAID_DATA, ReportType.entity): ScoreSkewHandlers.entity_hierarchy_skew,
    (JobType.PAID_DATA, ReportType.lifetime): ScoreSkewHandlers.lifetime_skew,
    (JobType.ORGANIC_DATA, ReportType.entity): ScoreSkewHandlers.entity_hierarchy_skew,
    (JobType.ORGANIC_DATA, ReportType.lifetime): ScoreSkewHandlers.organic_lifetime_skew,
    (JobType.PAID_DATA, ReportType.day): ScoreSkewHandlers.reporting_day_skew,
    (JobType.PAID_DATA, ReportType.day_age_gender): ScoreSkewHandlers.reporting_day_skew,
    (JobType.PAID_DATA, ReportType.day_dma): ScoreSkewHandlers.reporting_day_skew,
    (JobType.PAID_DATA, ReportType.day_region): ScoreSkewHandlers.reporting_day_skew,
    (JobType.PAID_DATA, ReportType.day_country): ScoreSkewHandlers.reporting_day_skew,
    (JobType.PAID_DATA, ReportType.day_hour): ScoreSkewHandlers.reporting_day_skew,
    (JobType.PAID_DATA, ReportType.day_platform): ScoreSkewHandlers.reporting_day_skew,
}

# You don't have to list all possible report types here.
# same_score is default if not on this list,
# but it helps to list possibilities for our record
SCORE_HISTORY_HANDLERS: Dict[Tuple[str, str], Callable[[ScorableClaim], float]] = {
    (JobType.PAID_DATA, ReportType.entity): ScoreSkewHandlers.history_ratio_entities,
    (JobType.PAID_DATA, ReportType.lifetime): ScoreSkewHandlers.history_ratio_lifetime,
    (JobType.ORGANIC_DATA, ReportType.entity): ScoreSkewHandlers.history_ratio_entities,
    (JobType.ORGANIC_DATA, ReportType.lifetime): ScoreSkewHandlers.history_ratio_lifetime,
    (JobType.PAID_DATA, ReportType.day): ScoreSkewHandlers.history_ratio_day_reports,
    (JobType.PAID_DATA, ReportType.day_age_gender): ScoreSkewHandlers.history_ratio_day_reports,
    (JobType.PAID_DATA, ReportType.day_dma): ScoreSkewHandlers.history_ratio_day_reports,
    (JobType.PAID_DATA, ReportType.day_region): ScoreSkewHandlers.history_ratio_day_reports,
    (JobType.PAID_DATA, ReportType.day_country): ScoreSkewHandlers.history_ratio_day_reports,
    (JobType.PAID_DATA, ReportType.day_hour): ScoreSkewHandlers.history_ratio_day_reports,
    (JobType.PAID_DATA, ReportType.day_platform): ScoreSkewHandlers.history_ratio_day_reports,
}


class ScoreCalculator:
    @staticmethod
    def skew_ratio(claim: ScorableClaim) -> float:
        job_type = detect_job_type(claim.report_type, claim.report_variant)
        fn = SCORE_SKEW_HANDLERS.get((job_type, claim.report_type), ScoreSkewHandlers.same_score)
        return fn(claim)

    @staticmethod
    def historical_ratio(claim: ScorableClaim) -> float:
        """Multiplier based on past efforts to download job."""
        job_type = detect_job_type(claim.report_type, claim.report_variant)
        fn = SCORE_HISTORY_HANDLERS.get((job_type, claim.report_type), ScoreSkewHandlers.same_score)
        return fn(claim)

    @classmethod
    def account_skew(cls, claim: ScorableClaim) -> float:
        # we allow individual AdAccounts to be marked with score multiplier
        if claim.entity_type == Entity.AdAccount and claim.entity_id:
            mult = AccountCache.get_score_multiplier(claim.entity_id)

            if mult is None:
                return 1.0
            else:
                return mult
        return 1.0

    @classmethod
    def assign_score(cls, claim: ScorableClaim) -> float:
        """Calculate score for a given claim."""
        if claim.report_type in ReportType.MUST_RUN_EVERY_SWEEP:
            return MUST_RUN_SCORE

        timer = Measure.timer(
            f'{__name__}.assign_score',
            tags={'entity_type': claim.entity_type, 'ad_account_id': claim.ad_account_id},
            sample_rate=0.01
        )

        with timer:
            hist_ratio = cls.historical_ratio(claim)
            score_skew_ratio = cls.skew_ratio(claim)
            account_skew = cls.account_skew(claim)

        combined_ratio = hist_ratio * score_skew_ratio * account_skew
        return int(MUST_RUN_SCORE * combined_ratio)


def iter_prioritized(claims: Iterable[ScorableClaim]) -> Generator[PrioritizationClaim, None, None]:
    """Assign score for each claim."""
    _measurement_name_base = f'{__name__}.{iter_prioritized.__name__}'

    _before_next_expectation = time.time()

    for claim in claims:
        _measurement_tags = {'entity_type': claim.entity_type, 'ad_account_id': claim.ad_account_id}

        Measure.timing(f'{_measurement_name_base}.next_expected', tags=_measurement_tags, sample_rate=0.01)(
            (time.time() - _before_next_expectation) * 1000
        )

        try:
            score = ScoreCalculator.assign_score(claim)
            with Measure.timer(f'{_measurement_name_base}.yield_result', tags=_measurement_tags):
                yield PrioritizationClaim(
                    claim.entity_id,
                    claim.entity_type,
                    claim.report_type,
                    claim.job_signature,
                    score,
                    ad_account_id=claim.ad_account_id,
                    timezone=claim.timezone,
                    range_start=claim.range_start,
                )
        except ScoringException as e:
            ErrorInspector.inspect(e, claim.ad_account_id, {'job_id': claim.job_id})

        _before_next_expectation = time.time()
