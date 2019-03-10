import time

from typing import Generator

from common.measurement import Measure
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.prioritizer.assign_score import assign_score

LAST = -1


def iter_prioritized(expectations_iter):
    # type: (Generator[ExpectationClaim]) -> Generator[PrioritizationClaim]
    """

    :param expectations_iter: generator yielding ExpectationClaim objects
    :type expectations_iter: Generator[ExpectationClaim]
    :return: Generator yielding PrioritizationClaim objects
    :rtype: Generator[PrioritizationClaim]
    """
    _measurement_name_base = __name__ + '.' + iter_prioritized.__name__
    _measurement_sample_rate = 1

    _before_next_expectation = time.time()
    for expectation_claim in expectations_iter:

        _measurement_tags = {
            'entity_type': expectation_claim.entity_type,
            'ad_account_id': expectation_claim.ad_account_id,
        }

        Measure.timing(
            _measurement_name_base + 'next_expected',
            tags=_measurement_tags,
            sample_rate=_measurement_sample_rate
        )((time.time() - _before_next_expectation)*1000)

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
        with Measure.timer(
            _measurement_name_base + 'assign_score',
            tags=_measurement_tags,
            sample_rate=_measurement_sample_rate
        ):
            last_task_score = assign_score(
                expectation_claim.job_signatures[LAST].job_id,
                expectation_claim.timezone
            )

        # score of zero is returned for all jobs in the beginning of the expectation_claim.job_signatures
        # list, and only the last job in the list gets an actual score
        job_scores = [0] * (len(expectation_claim.job_signatures) - 1) + [last_task_score]

        # This time includes the time consumer of this generator wastes
        # between reads from us. Good way to measure how quickly we are
        # consumed (what pauses we have between each consumption)
        with Measure.timer(
            _measurement_name_base + 'yield_result',
            tags=_measurement_tags,
            sample_rate=_measurement_sample_rate
        ):
            yield PrioritizationClaim(
                expectation_claim.to_dict(),
                job_scores=job_scores
            )

        _before_next_expectation = time.time()
