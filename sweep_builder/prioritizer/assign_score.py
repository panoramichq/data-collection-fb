from datetime import date, datetime
from collections import defaultdict, OrderedDict

from common.job_signature import JobSignature
from common.id_tools import parse_id, generate_id
from common.store.entityreport import FacebookEntityReport
from common.enums.reporttype import ReportType
from common.enums.failure_bucket import FailureBucket
from common.tztools import now_in_tz


def get_decay_ratio(units_in_past, decay_floor=0.15, rate=0.05):
    """
    Returns a number in the range from 1 to zero, indicating
    remaining portion of the score to keep given the reporting
    day is this far in the past.

    1
    ||
    | \
    |   \
    |    `"-..______
    ----------------
    0              X

    Used to discount significance of collection of data for older reporting periods

    :param units_in_past: Number of days/minutes/units record of interest is in the past compared to now.
    :param decay_floor: Proportion lower than which the score will NOT be reduced
    :param rate:
    :return:
    """
    # with above rate + floor, loses about half the score
    # in about 15 units, approaching the floor by about 50th unit
    return decay_floor + (1-decay_floor) * (1 - rate) ** units_in_past


def get_fade_in_ratio(units_in_past, rate=0.05):
    """
    Returns a number in range between zero and 1 indicating
    what portion of score to keep given the reporting time unit
    is this far in the past.

    1
    |     ,_.------
    |   /
    | /
    ||
    ----------------
    0              X

    :param units_in_past:
    :param rate:
    :return:
    """
    return 1 - (1 - rate) ** units_in_past


def get_minutes_away_from_whole_hour():
    minute = datetime.utcnow().minute
    if minute > 30:
        minute = 60 - minute
    return minute



class ScoreCalculator:

    def __init__(self):
        self._cache_hit_cnt = 0
        self._cache_not_hit_cnt = 0
        self._the_cache = OrderedDict()
        # loosely 2 years of days
        # What we are catching here is the-requested
        # job to collect some child level data per-Parent
        # in Case of entities that may be thousands, but
        # you need a cache of only 1 or 2 to deal with those
        # because for a stream of thousands job requests,
        # there are no temporal variants to that. Same for Lifetime
        # type of jobs.
        # The only time we need ~2 years of days cache space
        # when we deal with some daily reports, where Parent
        # will repeat but day may jump back and forth in range of
        # about 2-3 years (or whatever depth of collection we care about.
        self._max_cache_size = 600

    def assign_score(self, job_signature, timezone):
        # type: (JobSignature, str) -> int
        """

        :param JobSignature job_signature:
        :param str timezone:
        :return: score
        :rtype: int
        """

        job_id = job_signature.job_id

        prior_score = self._the_cache.get(job_id)
        if prior_score is not None:
            self._cache_hit_cnt += 1
            return prior_score

        job_id_components = parse_id(job_id)
        ad_account_id = job_id_components.get('ad_account_id')
        entity_id = job_id_components.get('entity_id')
        entity_type = job_id_components.get('entity_type')
        report_day = job_id_components.get('range_start')
        report_type = job_id_components.get('report_type')
        report_variant = job_id_components.get('report_variant')

        if report_type in ReportType.ALL_DAY_BREAKDOWNS:
            assert isinstance(report_day, date)
            coverage_period = report_day.strftime('%Y-%m-%d')
        else:
            coverage_period = report_type  # they don't have date.

        try:
            collection_record = FacebookEntityReport.get(
                generate_id(
                    ad_account_id=ad_account_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    report_type=report_type,
                    report_variant=report_variant
                ),
                coverage_period
            )
        # TODO: proper error here
        except:
            collection_record = None  # type: FacebookEntityReport

        score = 0

        if not entity_id:
            # per-parent
            if not collection_record:
                # for a group route that has no collection record,
                # this means we absolutely have to try it first (before per-element)
                score += 1000
            elif collection_record.last_success > collection_record.last_failure:
                # last group route was success. Let's try to keep it that way
                score += 30
            elif collection_record.last_failure_bucket == FailureBucket.Throttling:
                # not cool. we got clobbered by something jumping in front of us last time
                # let's try a little higher priority
                score += 100
            elif collection_record.last_failure_bucket == FailureBucket.TooLarge:
                # last time we tried this, report failed because we asked for
                # too much data and should probably not try us again.
                # however, if this was long time ago, maybe we should
                # to see if FB adjusted their API for these types of payloads
                # FB's release cycles are weekly (release on Tuesday)
                # Let's imagine that we should probably retry these failures if they are
                # 2+ weeks old
                days_since_failure = (datetime.utcnow() - collection_record.last_failure).days
                score += 10 * (days_since_failure / 14)
            else:
                # some sort of failure that we don't understand the meaning of right now
                # So, let's proceed with caution
                score += 5
        else:
            # per object
            if not collection_record:
                score += 20
            elif collection_record.last_success > collection_record.last_failure:
                # last group route was success. Let's try to keep it that way
                score += 10
            elif collection_record.last_failure_bucket == FailureBucket.Throttling:
                # not cool. we got clobbered by something jumping in front of us last time
                # let's try a little higher priority
                score += 80  # ever slightly less than per-parent approach
            elif collection_record.last_failure_bucket == FailureBucket.TooLarge:
                # last time we tried this, report failed because we asked for
                # too much data and should probably not try us again.
                # however, if this was long time ago, maybe we should
                # to see if FB adjusted their API for these types of payloads
                # FB's release cycles are weekly (release on Tuesday)
                # Let's imagine that we should probably retry these failures if they are
                # 2+ weeks old
                days_since_failure = (datetime.utcnow() - collection_record.last_failure).days
                score += 10 * min(2, days_since_failure / 14)
            else:
                # some sort of failure that we don't understand the meaning of right now
                # So, let's proceed with caution
                days_since_failure = (datetime.utcnow() - collection_record.last_failure).days
                score += 5 * min(3, days_since_failure / 14)

        if report_type in ReportType.ALL_DAY_BREAKDOWNS:
            # These are one record per day data points.
            # For these it's important NOT to recollect super
            # old, settled data.
            days_from_now = (now_in_tz(timezone).date() - report_day).days
            if days_from_now < 0:
                # which may happen if report_day is not in proper timezone
                days_from_now = 0
            score = score * get_decay_ratio(days_from_now, decay_floor=0.15, rate=0.05)
        elif report_type == ReportType.lifetime:
            # These don't have reporting day ranges.
            # But, these we need to try to collect "on the hour"
            # or as close to "on the hour" as possible.
            # On top of that, it's important to keep these fresh,
            # (preferably under an hour old). So, in-addition to
            # O-clock snapping, greater age == greater score.
            minutes = get_minutes_away_from_whole_hour()
            # with these ratios 80% of the score is gone by 8th minute away from whole hour
            score = score * get_decay_ratio(minutes, decay_floor=0, rate=0.2)
            # but now we need to boost it back

        if collection_record and collection_record.last_success:
            seconds_old = (datetime.utcnow() - collection_record.last_success).seconds
            # at this rate, 80% of score id regained by 15th minute
            # and ~100% by 36th minute.
            score = score * get_fade_in_ratio(seconds_old / 60, rate=0.1)

        score = int(score)

        if not entity_id:  # meaning NOT pertaining to single entity
            # If entity_id is set, chances of this thing being in the cache
            # under this particular report type / args are ZERO
            # No point putting it into cache or looking for it in the cache.
            # If we are here, it's some sort of GROUP report
            # - the only report that may repeat and for which it makes sense to
            # keep the cache of score
            self._cache_not_hit_cnt += 1
            self._the_cache[job_id] = score
            while len(self._the_cache) > self._max_cache_size:
                # removes oldest inserted item
                # https://docs.python.org/2/library/collections.html#collections.OrderedDict
                self._the_cache.popitem()

        return score
