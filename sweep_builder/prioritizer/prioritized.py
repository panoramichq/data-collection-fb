import logging
import math
import random
import time
from datetime import timedelta, datetime

from typing import Generator, Iterable, Tuple, Dict, Callable

from common.enums.jobtype import JobType, detect_job_type
from common.enums.reporttype import ReportType
from common.error_inspector import ErrorInspector
from common.measurement import Measure
from common.tztools import now
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.errors import ScoringException

logger = logging.getLogger(__name__)

MAX_SCORE_MULTIPLIER = 1.0
MIN_SCORE_MULTIPLIER = 0.5
JOB_MIN_SUCCESS_PERIOD_IN_DAYS = 30
JOB_MAX_AGE_IN_DAYS = 365 * 2
MUST_RUN_SCORE = 1000


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
    def lifetime_score(cls, claim: ScorableClaim) -> float:
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

    @staticmethod
    def day_based_metrics(claim: ScorableClaim) -> float:
        decay_per_day = 1 / (JOB_MAX_AGE_IN_DAYS * 2)
        today = now().date().toordinal()
        claim_day = claim.range_start.toordinal()
        days_in_past = today - claim_day
        mult = MAX_SCORE_MULTIPLIER * (1 - days_in_past * decay_per_day)

        if mult < MIN_SCORE_MULTIPLIER:
            return MIN_SCORE_MULTIPLIER
        else:
            return mult

    @staticmethod
    def random_half_skew(claim: ScorableClaim) -> float:
        return random.randrange(MAX_SCORE_MULTIPLIER/2, MAX_SCORE_MULTIPLIER)

# You don't have to list all possible report types here.
# same_score is default if not on this list,
# but it helps to list possibilities for our record
SCORE_SKEW_HANDLERS: Dict[Tuple[str, str], Callable[[ScorableClaim], float]] = {
    (JobType.PAID_DATA, ReportType.entity): ScoreSkewHandlers.random_half_skew,
    (JobType.PAID_DATA, ReportType.lifetime): ScoreSkewHandlers.lifetime_score,
    (JobType.ORGANIC_DATA, ReportType.entity): ScoreSkewHandlers.random_half_skew,
    (JobType.ORGANIC_DATA, ReportType.lifetime): ScoreSkewHandlers.lifetime_score,
    (JobType.PAID_DATA, ReportType.day): ScoreSkewHandlers.day_based_metrics,
    (JobType.PAID_DATA, ReportType.day_age_gender): ScoreSkewHandlers.day_based_metrics,
    (JobType.PAID_DATA, ReportType.day_dma): ScoreSkewHandlers.day_based_metrics,
    (JobType.PAID_DATA, ReportType.day_region): ScoreSkewHandlers.day_based_metrics,
    (JobType.PAID_DATA, ReportType.day_country): ScoreSkewHandlers.day_based_metrics,
    (JobType.PAID_DATA, ReportType.day_hour): ScoreSkewHandlers.day_based_metrics,
    (JobType.PAID_DATA, ReportType.day_platform): ScoreSkewHandlers.day_based_metrics,
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
        last_success_dt = claim.last_report.last_success_dt if claim.last_report else None

        if not last_success_dt:
            return MAX_SCORE_MULTIPLIER
        else:
            # cool! prior success!
            # **the further in the past is prior success the higher is
            # the need to recollect / refresh**
            # (but caps out on constant some ~30 days in past)
            # (not same thing as "the futher in past is the reporting date of data". Separate score there)

            # most recently-successfully collected data score gets close to zero score
            max_dt = now()
            # Absolute minimum is success every N days
            min_dt = max_dt - timedelta(days=JOB_MIN_SUCCESS_PERIOD_IN_DAYS)
            if last_success_dt <= min_dt:
                return MAX_SCORE_MULTIPLIER
            else: #  between min and max
                min_timestamp = min_dt.timestamp()
                max_timestamp = max_dt.timestamp()
                last_success_timestamp = last_success_dt.timestamp()
                return min(
                    MAX_SCORE_MULTIPLIER * (max_timestamp - last_success_timestamp) / (max_timestamp - min_timestamp),
                    MIN_SCORE_MULTIPLIER
                )

    @classmethod
    def assign_score(cls, claim: ScorableClaim) -> int:
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

        combined_ratio = hist_ratio * score_skew_ratio
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
