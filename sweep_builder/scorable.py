import logging

from collections import defaultdict
from typing import Iterable, Generator, Optional

from pynamodb.exceptions import DoesNotExist

from common.enums.jobtype import detect_job_type
from config.application import PERMANENTLY_FAILING_JOB_THRESHOLD
from config.jobs import FAILS_IN_ROW_BREAKDOWN_LIMIT, TASK_BREAKDOWN_ENABLED
from common.enums.failure_bucket import FailureBucket
from common.measurement import Measure
from common.store.jobreport import JobReport
from common.id_tools import generate_id
from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim

logger = logging.getLogger(__name__)


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
    if report.fails_in_row is None or report.fails_in_row < FAILS_IN_ROW_BREAKDOWN_LIMIT:
        return True

    return False


def generate_scorable(claim: ExpectationClaim) -> Generator[ScorableClaim, None, None]:
    """Select job signature for single expectation claim."""
    last_report = _fetch_job_report(claim.job_id)
    if not TASK_BREAKDOWN_ENABLED or not claim.is_divisible or last_report is None or should_select(last_report):
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
