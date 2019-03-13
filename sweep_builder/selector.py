import functools

from typing import Iterable, Generator, Optional

from common.job_signature import JobSignature
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
    # TODO: should we use this signature based on job history
    if report is None:
        # not ran yet
        return True

    # TODO: Implement logic that decides whether job should be broken down or not
    return True


def select_signature(claim: ExpectationClaim) -> (JobSignature, JobReport):
    """Select job signature for single expectation claim."""
    for signature in claim.job_signatures[:-1]:
        report = _fetch_job_report(signature.job_id)
        if should_select(signature, report):
            return signature, report

    # default to last signature
    last_signature = claim.job_signatures[-1]
    last_report = _fetch_job_report(last_signature.job_id)
    return last_signature, last_report


def iter_select_signature(claims: Iterable[ExpectationClaim]) -> Generator[ScorableClaim, None, None]:
    """Select signature for each expectation claim based on job history."""
    for claim in claims:
        signature, report = select_signature(claim)
        # TODO: to_dict copying below
        yield ScorableClaim(
            claim.to_dict(),
            selected_signature=signature,
            last_report=report,
        )
