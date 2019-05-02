import logging
from typing import Dict

from common.enums.entity import Entity

from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.measurement import Measure
from common.tztools import now_in_tz, now
from common.math import adapt_decay_rate_to_population, get_decay_proportion, get_fade_in_proportion
from config.jobs import ACTIVATE_JOB_GATEKEEPER
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper

# This controls score decay for insights that are day-specific
# the further in the past, the less we care.
# The edge of what we care about is deemed to be \/ 2 years.

DAYS_BACK_DECAY_RATE = adapt_decay_rate_to_population(365 * 2)
MINUTES_AWAY_FROM_WHOLE_HOUR_DECAY_RATE = adapt_decay_rate_to_population(30)

logger = logging.getLogger(__name__)


def _extract_tags_from_claim(claim: ScorableClaim, *_, **__) -> Dict[str, str]:
    return {'entity_type': claim.entity_type, 'ad_account_id': claim.ad_account_id}


def get_minutes_away_from_whole_hour() -> int:
    minute = now().minute
    if minute > 30:
        minute = 60 - minute
    return minute


# cache_max_size allows us to avoid writing same score
# for same jobID when given objects rely on same JobID
# for collection.
# This number is
#  max Expectations permutations per Reality Claim (~6k Fandango ads)
#  x
#  margin of comfort (say, 3)
#  ========
#  ~20k
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_from_claim)
def assign_score(claim: ScorableClaim) -> int:
    """Calculate score for a given job."""
    timezone = claim.timezone
    last_report = claim.last_report
    report_day = claim.range_start
    report_type = claim.report_type
    report_variant = claim.report_variant

    if claim.report_type in ReportType.MUST_RUN_EVERY_SWEEP:
        return 1000

    # if we are here, we have Platform-flavored job
    is_per_page_metrics_job = report_variant in Entity.NON_AA_SCOPED

    if ACTIVATE_JOB_GATEKEEPER and not JobGateKeeper.shall_pass(claim):
        return JobGateKeeper.JOB_NOT_PASSED_SCORE

    score = 0

    if claim.is_per_parent_job or is_per_page_metrics_job:
        # yeah, i know, redundant, but keeping it here
        # to allow per-entity_id logic further below to be around

        if not last_report:
            # for a group route that has no collection record,
            # this means we absolutely have to try it first (before per-element)
            score += 1000
        else:

            # Happy outcomes

            if last_report.last_success_dt and not last_report.last_failure_dt:
                # perfect record of success in fetching
                # TODO: decay this based on time here, instead of below, maybe...
                score += 10
            elif (
                last_report.last_success_dt
                and last_report.last_failure_dt
                and last_report.last_success_dt > last_report.last_failure_dt
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

            elif last_report.last_failure_dt:
                if last_report.last_failure_bucket == FailureBucket.Throttling:
                    # not cool. it was important to us on prior runs, but
                    # we got clobbered by something jumping in front of us last time
                    # let's try a little higher priority
                    score += 100
                elif last_report.last_failure_bucket == FailureBucket.TooLarge:
                    # last time we tried this, report failed because we asked for
                    # too much data and should probably not try us again.
                    # however, if this was long time ago, maybe we should
                    # to see if FB adjusted their API for these types of payloads
                    # FB's release cycles are weekly (release on Tuesday)
                    # Let's imagine that we should probably retry these failures if they are
                    # 2+ weeks old
                    days_since_failure = (now() - last_report.last_failure_dt).days
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
        # per entity_id
        # this is not used now, but is left for reuse when we unleash per-entity_id jobs
        # onto this code again. Must be revisited
        if not last_report:
            score += 20
        elif (
            last_report.last_success_dt
            and last_report.last_failure_dt
            and last_report.last_success_dt > last_report.last_failure_dt
        ):
            # last group route was success. Let's try to keep it that way
            score += 10
        elif last_report.last_failure_bucket == FailureBucket.Throttling:
            # not cool. we got clobbered by something jumping in front of us last time
            # let's try a little higher priority
            score += 80  # ever slightly less than per-parent approach
        elif last_report.last_failure_bucket == FailureBucket.TooLarge:
            # last time we tried this, report failed because we asked for
            # too much data and should probably not try us again.
            # however, if this was long time ago, maybe we should
            # to see if FB adjusted their API for these types of payloads
            # FB's release cycles are weekly (release on Tuesday)
            # Let's imagine that we should probably retry these failures if they are
            # 2+ weeks old
            days_since_failure = (now() - last_report.last_failure_dt).days
            score += 10 * min(2, days_since_failure / 14)
        else:
            # some sort of failure that we don't understand the meaning of right now
            # So, let's proceed with caution
            days_since_failure = (now() - last_report.last_failure_dt).days
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
            days_from_now, rate=DAYS_BACK_DECAY_RATE, decay_floor=0.10  # never decay to lower then 10% of the score
        )

    elif report_type == ReportType.lifetime:
        # Do not decrease the score for lifetime reports. Only decrease it based on last_success_dt, which
        # happens in the block below. At minimum we should collect lifetime reports once in two hours.
        pass

    if last_report and last_report.last_success_dt:
        seconds_old = (now() - last_report.last_success_dt).seconds
        # at this rate, 80% of score id regained by 15th minute
        # and ~100% by 36th minute.
        score = score * get_fade_in_proportion(seconds_old / 60, rate=0.1)

    score = int(score)

    return score
