import functools

from collections import defaultdict

from typing import Iterable, Generator, Optional

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
    return JobReport.get(job_id)


def should_select(report: Optional[JobReport]) -> bool:
    """Decide if signature should be used based on last report."""
    # not ran yet
    if report is None:
        return True

    # only break down jobs with too large error
    if report.last_failure_bucket != FailureBucket.TooLarge:
        return True

    # need to fail n-times in a row
    if report.fails_in_row is None or report.fails_in_row < FAILS_IN_ROW_BREAKDOWN_LIMIT:
        return True

    return False


def select_signature(claim: ExpectationClaim) -> ScorableClaim:
    """Select job signature for single expectation claim."""
    for signature in claim.effective_job_signatures:
        report = _fetch_job_report(signature.job_id)
        if should_select(report):
            return ScorableClaim(claim.to_dict(), selected_job_signature=signature, last_report=report)

    # default to normative signature
    return ScorableClaim(
        claim.to_dict(),
        selected_job_signature=claim.normative_job_signature,
        last_report=_fetch_job_report(claim.normative_job_id),
    )


def iter_select_signature(claims: Iterable[ExpectationClaim]) -> Generator[ScorableClaim, None, None]:
    """Select signature for each expectation claim based on job history."""
    histogram_counter = defaultdict(int)
    for claim in claims:
        yield select_signature(claim)
        histogram_counter[(claim.ad_account_id, claim.entity_type)] += 1

    for ((ad_account_id, entity_type), count) in histogram_counter.items():
        Measure.histogram(
            f'{__name__}.{iter_select_signature.__name__}.scorable_claims_per_expectation_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type},
        )(count)
