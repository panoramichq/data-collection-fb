import functools

from collections import defaultdict

from typing import Iterable, Generator, Optional

from pynamodb.exceptions import DoesNotExist

from common.enums.failure_bucket import FailureBucket
from config.jobs import FAILS_IN_ROW_BREAKDOWN_LIMIT

from common.measurement import Measure
from common.store.jobreport import JobReport
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim


# TODO: add maxsize to cache below?
@functools.lru_cache(maxsize=None)
def _fetch_job_report(job_id: str) -> Optional[JobReport]:
    """Retrieve job report from job report table (cached)."""
    try:
        return JobReport.get(job_id)
    except DoesNotExist:
        return None


def should_select(report: JobReport) -> bool:
    """Decide if signature should be used based on last report."""
    # only break down jobs with too large error
    if report.last_failure_bucket != FailureBucket.TooLarge:
        return True

    # need to fail n-times in a row
    if report.fails_in_row is None or report.fails_in_row < FAILS_IN_ROW_BREAKDOWN_LIMIT:
        return True

    return False


def select_signature(claim: ExpectationClaim) -> Generator[ScorableClaim, None, None]:
    """Select job signature for single expectation claim."""
    signature = (
        claim.effective_job_signature if claim.effective_job_signature is not None else claim.normative_job_signature
    )

    report = _fetch_job_report(signature.job_id)
    if not claim.is_divisible or report is None or should_select(report):
        yield ScorableClaim(claim.to_dict(), selected_job_signature=signature, last_report=report)
        return

    # break down into smaller jobs recursively
    for child_claim in claim.generate_child_claims():
        yield from select_signature(child_claim)


def iter_select_signature(claims: Iterable[ExpectationClaim]) -> Generator[ScorableClaim, None, None]:
    """Select signature for each expectation claim based on job history."""
    histogram_counter = defaultdict(int)
    for claim in claims:
        yield from select_signature(claim)
        histogram_counter[(claim.ad_account_id, claim.entity_type)] += 1

    for ((ad_account_id, entity_type), count) in histogram_counter.items():
        Measure.histogram(
            f'{__name__}.{iter_select_signature.__name__}.scorable_claims_per_expectation_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type},
        )(count)
