from typing import Generator, Callable

from sweep_builder.expectation_builder.expectations import iter_expectations, ExpectationClaim

from .prioritization_claim import PrioritizationClaim
from .assign_score import assign_score


def iter_prioritized(iter_expectations=iter_expectations):
    # type: (Callable[..., Generator[ExpectationClaim]]) -> Generator[PrioritizationClaim]
    """

    :param iter_expectations: Callable that returns generator yielding ExpectationClaim objects
    :type iter_expectations: () -> Generator[ExpectationClaim]
    :return: Generator yielding PrioritizationClaim objects
    :rtype: Generator[PrioritizationClaim]
    """

    for expectation_claim in iter_expectations():
        yield PrioritizationClaim(
            expectation_claim.to_dict(),
            job_scores = [
                assign_score(job_signature, expectation_claim.timezone)
                for job_signature in expectation_claim.job_signatures
            ]
        )
