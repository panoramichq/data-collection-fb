import logging

from collections import defaultdict
from typing import Iterable, Generator, Optional

from pynamodb.exceptions import DoesNotExist

from common.enums.jobtype import detect_job_type
from common.store.entities import AdAccountEntity
from config.application import PERMANENTLY_FAILING_JOB_THRESHOLD
from config import jobs as jobs_config # for ease of mocking in tests
from common.enums.failure_bucket import FailureBucket
from common.measurement import Measure
from common.store.jobreport import JobReport
from common.id_tools import generate_id
from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.prioritizer.gatekeeper import JobGateKeeperCache

logger = logging.getLogger(__name__)

class NotSet:
    pass


def _fetch_job_report(job_id: str) -> Optional[JobReport]:
    """Retrieve job report from job report table (cached)."""
    try:
        report = JobReport.get(job_id)
        if report.fails_in_row and report.fails_in_row >= PERMANENTLY_FAILING_JOB_THRESHOLD:
            Measure.counter('permanently_failing_job').increment()
            logger.warning(
                f'[permanently-failing-job] Job with id {job_id} failed {report.fails_in_row}' f' times in a row.'
            )
        return report
    except DoesNotExist:
        return None


def generate_child_claims(claim: ExpectationClaim) -> Generator[ExpectationClaim, None, None]:
    for child_entity_node in claim.entity_hierarchy.children:
        yield ExpectationClaim(
            child_entity_node.entity_id,
            child_entity_node.entity_type,
            claim.report_type,
            claim.report_variant,
            JobSignature(
                generate_id(
                    ad_account_id=claim.ad_account_id,
                    range_start=claim.range_start,
                    report_type=claim.report_type,
                    report_variant=claim.report_variant,
                    entity_id=child_entity_node.entity_id,
                    entity_type=child_entity_node.entity_type,
                )
            ),
            ad_account_id=claim.ad_account_id,
            timezone=claim.timezone,
            entity_hierarchy=child_entity_node,
            range_start=claim.range_start,
        )


def should_select(report: JobReport) -> bool:
    """Decide if signature should be used based on last report."""
    # only break down jobs with too large error
    if report.last_failure_bucket != FailureBucket.TooLarge:
        return True

    # need to fail n-times in a row
    if report.fails_in_row is None or report.fails_in_row < jobs_config.FAILS_IN_ROW_BREAKDOWN_LIMIT:
        return True

    return False


class RecollectFlagHelper:

    _cache = {} # ad_account_id : Union[value of recollect_records_older_than | NotSet ]

    @classmethod
    def get_recollect_if_older_value(cls, ad_account_id):
        value = cls._cache.get(ad_account_id)
        if value is None:
            try:
                # may be None or some datetime (as string or for reals)
                value = AdAccountEntity.get(ad_account_id).recollect_records_older_than
            except DoesNotExist:
                value = None
            cls._cache[ad_account_id] = NotSet if value is None else value
        return value

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()


def resolve_last_report(claim: ExpectationClaim) -> Optional[JobReport]:
    # If "recollect" flag is on AdAccount in a form of
    # AdAccout.recollect_records_older_than attribute having non-Null value
    # 1, load JobReport first
    # 2. compare JobReport attempt time to "recollect if older" value
    # 3.a If JobReport is older, disregard it and do NOT hit JobGateKeeperCache.shall_pass
    # 3.b if JobReport is newer, pipe through JobGateKeeperCache.shall_pass

    # If "recollect" is NOT set on AdAccount,
    # slight change in order of optimizations
    # 1 check with JobGateKeeperCache.shall_pass(claim.job_id) first
    # 2. _fetch_job_report(claim.job_id) next

    # Goes without saying, it's better if "recollect" flags are cleared once we see records update.
    # Otherwise it takes logic onto a bit more Dynamo-based (and slower / error-prone) path.

    recollect_records_older_than = None
    if claim.ad_account_id:
        recollect_records_older_than = RecollectFlagHelper.get_recollect_if_older_value(claim.ad_account_id)

    if recollect_records_older_than:
        last_report = _fetch_job_report(claim.job_id)
        if last_report is None:
            pass
        elif last_report.last_progress_dt < recollect_records_older_than:
            last_report = None
        elif jobs_config.ACTIVATE_JOB_GATEKEEPER and not JobGateKeeperCache.shall_pass(claim.job_id):
            last_report = None
        else:
            pass # last_report is what it is
    else:
        if jobs_config.ACTIVATE_JOB_GATEKEEPER and not JobGateKeeperCache.shall_pass(claim.job_id):
            last_report = None
        else:
            last_report = _fetch_job_report(claim.job_id)

    return last_report


def generate_scorable(claim: ExpectationClaim) -> Generator[ScorableClaim, None, None]:
    """Select job signature for single expectation claim."""

    last_report = resolve_last_report(claim)

    if not jobs_config.TASK_BREAKDOWN_ENABLED or not claim.is_divisible or last_report is None or should_select(last_report):
        yield ScorableClaim(
            claim.entity_id,
            claim.entity_type,
            claim.report_type,
            claim.report_variant,
            claim.job_signature,
            last_report,
            ad_account_id=claim.ad_account_id,
            timezone=claim.timezone,
            range_start=claim.range_start,
        )
        return

    logger.warning(f'Performing task breakdown for job_id: {claim.job_id}')
    Measure.increment(
        f'{__name__}.{generate_scorable.__name__}.task_broken_down',
        tags={'ad_account_id': claim.ad_account_id, 'entity_type': claim.entity_type},
    )(1)

    # break down into smaller jobs recursively
    for child_claim in generate_child_claims(claim):
        yield from generate_scorable(child_claim)


def iter_scorable(claims: Iterable[ExpectationClaim]) -> Generator[ScorableClaim, None, None]:
    """Select signature for each expectation claim based on job history."""
    histogram_counter = defaultdict(int)
    for claim in claims:
        for scorable_claim in generate_scorable(claim):
            job_type = detect_job_type(claim.report_type, claim.entity_type)
            histogram_counter[(claim.ad_account_id, claim.entity_type, job_type)] += 1
            yield scorable_claim

    for ((ad_account_id, entity_type, job_type), count) in histogram_counter.items():
        Measure.histogram(
            f'{__name__}.{iter_scorable.__name__}.scorable_claims_per_expectation_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type, 'job_type': job_type},
        )(count)
