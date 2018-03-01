from typing import Generator, Callable

from sweep_builder.expectation_builder.expectations import iter_expectations, ExpectationClaim

from .prioritization_claim import PrioritizationClaim
from .assign_score import ScoreCalculator


LAST = -1


def iter_prioritized(iter_expectations=iter_expectations):
    # type: (Callable[..., Generator[ExpectationClaim]]) -> Generator[PrioritizationClaim]
    """

    :param iter_expectations: Callable that returns generator yielding ExpectationClaim objects
    :type iter_expectations: () -> Generator[ExpectationClaim]
    :return: Generator yielding PrioritizationClaim objects
    :rtype: Generator[PrioritizationClaim]
    """

    score_calculator = ScoreCalculator()

    for expectation_claim in iter_expectations():

        # Original logic
        # Temporarily ignored because of the focus in Persister component on last job in the list only
        # since we know only the last job will be queued, no sense estimating the score for others.
        # TODO: obviously revert to this when Persister starts writing out all jobs in expectation_claim.job_signatures
        # job_scores = [
        #     score_calculator.assign_score(job_signature, expectation_claim.timezone)
        #     for job_signature in expectation_claim.job_signatures
        # ]

        # temporarily ignoring normative jobs and just going after the most abstract
        # effective job that satisfies the normative objective.
        # It's usually the last one in the list. If there is *only* a normative
        # job in the list, it's also the last one in the list.
        last_task_score = score_calculator.assign_score(
            expectation_claim.job_signatures[LAST],
            expectation_claim.timezone
        )
        # score of zero is returned for all jobs in the beginning of the expectation_claim.job_signatures
        # list, and only the last job in the list gets an actual score
        job_scores = [0 for i in range(0, len(expectation_claim.job_signatures)-1)] + [last_task_score]

        yield PrioritizationClaim(
            expectation_claim.to_dict(),
            job_scores=job_scores
        )
