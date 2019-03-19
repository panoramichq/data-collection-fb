import functools
from collections import defaultdict

from typing import Iterable, Generator, Optional

from common.job_signature import JobSignature
from common.measurement import Measure
from common.store.jobreport import JobReport
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim


# TODO: add maxsize to cache below?
@functools.lru_cache(maxsize=None)
def _fetch_job_report(job_id: str) -> Optional[JobReport]:
    """Retrieve job report from job report table (cached)."""
    return JobReport.get(job_id)


def should_select(signature: JobSignature, report: Optional[JobReport]) -> bool:
    """Decide if signature should be used based on last report."""
    if report is None:
        # not ran yet
        return True

    # TODO: Implement logic that decides whether job should be broken down or not
    return True


def select_signature(claim: ExpectationClaim) -> ScorableClaim:
    """Select job signature for single expectation claim."""
    for signature in claim.effective_job_signatures:
        report = _fetch_job_report(signature.job_id)
        if should_select(signature, report):
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

    for ((ad_account_id, entity_type), count) in histogram_counter:
        Measure.histogram(
            f'{__name__}.{iter_select_signature.__name__}.scorable_claims_per_expectation_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type},
            sample_rate=1,
        )(count)
