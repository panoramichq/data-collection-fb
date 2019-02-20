import random

from collections import OrderedDict
from datetime import datetime
from typing import Optional

import config.application
from common.enums.entity import Entity

from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.id_tools import parse_id_parts, JobIdParts
from common.job_signature import JobSignature
from common.store.jobreport import JobReport
from common.tztools import now_in_tz, now
from common.math import (
    adapt_decay_rate_to_population,
    get_decay_proportion,
    get_fade_in_proportion,
)

# This controls score decay for insights that are day-specific
# the further in the past, the less we care.
# The edge of what we care about is deemed to be \/ 2 years.
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper

DAYS_BACK_DECAY_RATE = adapt_decay_rate_to_population(365 * 2)
MINUTES_AWAY_FROM_WHOLE_HOUR_DECAY_RATE = adapt_decay_rate_to_population(30)


def get_minutes_away_from_whole_hour():
    minute = now().minute
    if minute > 30:
        minute = 60 - minute
    return minute


class ScoreCalculator:

    def __init__(self, cache_size=4000):
        self._cache_hit_cnt = 0
        self._cache_not_hit_cnt = 0
        self._the_cache = OrderedDict()

        # 3 years of days: dd = ~1000
        # 4 daily report types: ddrt = 4
        # 4 day-less (lifetime or entities pull): srt = 4
        # average possible number of children per AA: ch = 2000 C + 10,000 AS + 15,000 A

        # When we don't do per-entity jobs scoring,
        # possible average combinations of jobs per ad account is
        # 1,000 x 4 + 4 = 4,004.

        # When you permute against possibility of running this against
        # all entities individually you are getting into
        # 4,000 x 30,000 = 120,000,000 possible combinations
        # of job_ids per ad account.

        # Luckily, at this time, we don't release per-entity-id tasks (yet)
        # so we can easily get by with a cache size of ~4000 items
        # and know that that will be enough to score entire single ad account
        # TODO: when we start releasing per-entity_id jobs, must tighten what you put into cache
        #       Putting only per-parent jobs in there makes sense.
        #       As only those are reuses across children.

        self._max_cache_size = cache_size

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

        # for convenience of reading of the code below,
        # exploding the job id parts into individual vars
        job_id_parts = parse_id_parts(job_id)
        ad_account_id = job_id_parts.ad_account_id
        entity_id = job_id_parts.entity_id
        entity_type = job_id_parts.entity_type
        report_day = job_id_parts.range_start
        report_type = job_id_parts.report_type
        report_variant = job_id_parts.report_variant

        if job_id_parts.namespace == config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE:
            # some system worker. must run on every sweep usually
            # give it highest score to give it a good chance.
            return 1000

        if job_id_parts.report_type in ReportType.MUST_RUN_EVERY_SWEEP:
            return 1000

        # if we are here, we have Platform-flavored job
        is_per_parent_job = bool(not entity_type and report_variant)

        if not is_per_parent_job and ad_account_id != '23845179':
            # at this time, it's impossible to have per-entity_id
            # jobs here becase sweep builder specifically avoids
            # scoring and releasing per-entity_id jobs
            # TODO: when we get per-entity_id jobs back, do some scoring for these
            # Until then, we are making sure per-parent jobs get out first
            return 0

        try:
            collection_record = JobReport.get(job_id)  # type: JobReport
        except:  # TODO: proper error catching here
            collection_record = None  # type: JobReport

        last_success_dt = None if collection_record is None else collection_record.last_success_dt
        if not JobGateKeeper.shall_pass(job_id_parts, last_success_dt=last_success_dt):
            return JobGateKeeper.JOB_NOT_PASSED_SCORE

        score = 0

        if ad_account_id == '23845179' and report_type != ReportType.entity:
            now_time = now_in_tz(timezone)
            if not collection_record or not collection_record.last_success_dt:
                # Not succeeded this job yet
                return random.randint(8, 15)
            else:
                secs_since_last_success = (now_time - collection_record.last_success_dt).seconds
                if secs_since_last_success > 60 * 60 * 8:
                    # Succeeded more than 8 hours ago
                    return random.randint(8, 15)
                else:
                    # Succeeded in last 8 hours
                    return 0

        if is_per_parent_job:
            # yeah, i know, redundant, but keeping it here
            # to allow per-entity_id logic further below to be around

            if not collection_record:
                # for a group route that has no collection record,
                # this means we absolutely have to try it first (before per-element)
                score += 1000
            else:

                # Happy outcomes

                if collection_record.last_success_dt and not collection_record.last_failure_dt:
                    # perfect record of success in fetching
                    # TODO: decay this based on time here, instead of below, maybe...
                    score += 10
                elif (
                    collection_record.last_success_dt and collection_record.last_failure_dt
                    and collection_record.last_success_dt > collection_record.last_failure_dt
                ):
                    # Some history of recent success, but also record of failures,
                    # meaning next time we schedule this one, it may error our
                    # let's speculatively schedule this guy a bit higher to give it
                    # greater success rate on average:
                    # TODO: implement decay on "some failure" effect. Otherwise jobs with
                    #       one failure in their life will forever be scored higher.
                    score += 20

                # here we enter unhappy territory
                # either no or old history of success, overshadowed by failure or nothingness

                elif collection_record.last_failure_dt:
                    if collection_record.last_failure_bucket == FailureBucket.Throttling:
                        # not cool. it was important to us on prior runs, but
                        # we got clobbered by something jumping in front of us last time
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
                        days_since_failure = (now() - collection_record.last_failure_dt).days
                        score += 10 * (days_since_failure / 14)
                    else:
                        # some other failure. Not sure what approach to take, but
                        # caution would probably be proper.
                        score += 5

                else:
                    # likely "in progress" still. let's wait for it to die or success
                    # TODO: implement decay on recency of last "in progress".
                    #       Otherwise jobs that failed silently after some progress will never revive themselves
                    score += 5

        else:
            if entity_type == Entity.AdAccount:
                # This is an ad account sync job, let's rank it a bit higher as
                # these updates ar quite important
                score += 100

            # per entity_id
            # this is not used now, but is left for reuse when we unleash per-entity_id jobs
            # onto this code again. Must be revisited
            if not collection_record:
                score += 20
            elif collection_record.last_success_dt > collection_record.last_failure_dt:
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
                days_since_failure = (now() - collection_record.last_failure_dt).days
                score += 10 * min(2, days_since_failure / 14)
            else:
                # some sort of failure that we don't understand the meaning of right now
                # So, let's proceed with caution
                days_since_failure = (now() - collection_record.last_failure_dt).days
                score += 5 * min(3, days_since_failure / 14)

        if report_type in ReportType.ALL_DAY_BREAKDOWNS:
            # These are one record per day data points.
            # For these it's important NOT to recollect super
            # old, settled data.
            days_from_now = (now_in_tz(timezone).date() - report_day).days
            if days_from_now < 0:
                # which may happen if report_day is not in proper timezone
                days_from_now = 0
            score = score * get_decay_proportion(
                days_from_now,
                rate=DAYS_BACK_DECAY_RATE,
                decay_floor=0.10  # never decay to lower then 10% of the score
            )

        elif report_type == ReportType.lifetime:
            # Do not decrease the score for lifetime reports. Only decrease it based on last_success_dt, which
            # happens in the block below. At minimum we should collect lifetime reports once in two hours.
            pass

        if collection_record and collection_record.last_success_dt:
            seconds_old = (now() - collection_record.last_success_dt).seconds
            # at this rate, 80% of score id regained by 15th minute
            # and ~100% by 36th minute.
            score = score * get_fade_in_proportion(seconds_old / 60, rate=0.1)

        score = int(score)

        if is_per_parent_job:  # meaning NOT pertaining to single entity
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


class JobGateKeeper:
    """Prevent over-collection of datapoints."""

    JOB_NOT_PASSED_SCORE = 1

    @staticmethod
    def shall_pass(job: JobIdParts, last_success_dt: Optional[datetime]):
        """Return true if job should be re-collected."""
        if last_success_dt is None or job.bol is None:
            return True

        # Single-day jobs have no range_end
        job_range_end = job.range_start if job.range_end is None else job.range_end

        minutes_since_success = (now() - last_success_dt).total_seconds() / 60
        datapoint_age_in_days = (now().date() - job_range_end).total_seconds() / (60 * 60 * 24)

        if datapoint_age_in_days < 3:
            return True
        elif datapoint_age_in_days < 7:
            return JobGateKeeper._every_x_hours(minutes_since_success, 1)
        elif datapoint_age_in_days < 14:
            return JobGateKeeper._every_x_hours(minutes_since_success, 5)
        elif datapoint_age_in_days < 30:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24)
        elif datapoint_age_in_days < 90:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24 * 3)
        else:
            return JobGateKeeper._every_x_hours(minutes_since_success, 24 * 7)

    @staticmethod
    def _every_x_hours(minutes_since_success, x_hours):
        return minutes_since_success > 60 * x_hours
