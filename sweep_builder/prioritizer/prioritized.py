import time

from typing import Generator

from common.measurement import Measure
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim

from .assign_score import ScoreCalculator


LAST = -1


def iter_prioritized(expectations_iter):
    # type: (Generator[ExpectationClaim]) -> Generator[PrioritizationClaim]
    """

    :param expectations_iter: generator yielding ExpectationClaim objects
    :type expectations_iter: Generator[ExpectationClaim]
    :return: Generator yielding PrioritizationClaim objects
    :rtype: Generator[PrioritizationClaim]
    """

    score_calculator = ScoreCalculator()

    _measurement_name_base = __name__ + '.iter_prioritized.'  # <- function name. adjust if changed
    next_expectation_timer = Measure.timing(
        _measurement_name_base + 'fetch_expectation',
        sample_rate=0.1
    )
    assign_score_timer = Measure.timing(
        _measurement_name_base + 'assign_score',
        sample_rate=0.1
    )

    _before_next_expectation = time.time()
    for expectation_claim in expectations_iter:
        next_expectation_timer(time.time() - _before_next_expectation)

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
        _before_score = time.time()
        last_task_score = score_calculator.assign_score(
            expectation_claim.job_signatures[LAST],
            expectation_claim.timezone
        )
        assign_score_timer(time.time() - _before_score)

        # score of zero is returned for all jobs in the beginning of the expectation_claim.job_signatures
        # list, and only the last job in the list gets an actual score
        job_scores = [0 for i in range(0, len(expectation_claim.job_signatures)-1)] + [last_task_score]

        with Measure.timer(_measurement_name_base + 'yield_result', sample_rate=0.1):
            yield PrioritizationClaim(
                expectation_claim.to_dict(),
                job_scores=job_scores
            )

        _before_next_expectation = time.time()
