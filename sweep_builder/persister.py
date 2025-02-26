import logging
import time
from collections import defaultdict

from typing import Generator, Iterable

from common.enums.jobtype import detect_job_type
from common.measurement import Measure
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.account_cache import AccountCache

logger = logging.getLogger(__name__)

JOB_NOT_PASSED_SCORE = 1


def should_persist(job_score: int) -> bool:
    """Determine whether job with score should be persisted."""
    return job_score > JOB_NOT_PASSED_SCORE


def iter_persist_prioritized(
    sweep_id: str, prioritized_iter: Iterable[PrioritizationClaim]
) -> Generator[PrioritizationClaim, None, None]:
    """Persist prioritized jobs and pass-through context objects for inspection."""

    AccountCache.reset()

    with SortedJobsQueue(sweep_id).JobsWriter() as add_to_queue:

        _measurement_name_base = f'{__name__}.{iter_persist_prioritized.__name__}'

        _before_next_prioritized = time.time()
        for prioritization_claim in prioritized_iter:
            job_type = detect_job_type(prioritization_claim.report_type, prioritization_claim.entity_type)
            _measurement_tags = {
                'entity_type': prioritization_claim.entity_type,
                'report_type': prioritization_claim.report_type,
                'ad_account_id': prioritization_claim.ad_account_id,
                'job_type': job_type,
                'sweep_id': sweep_id,
            }

            Measure.timing(f'{_measurement_name_base}.next_prioritized', tags=_measurement_tags, sample_rate=0.01)(
                (time.time() - _before_next_prioritized) * 1000
            )

            score = prioritization_claim.score
            if not should_persist(score):
                logger.debug(f'Not persisting job {prioritization_claim.job_id} due to low score: {score}')
                continue

            # Following are JobScope attributes we don't store on JobID
            # so we need to store them separately.
            # See JobScope object for exact attr names.
            # At this point persister forms the
            # auxiliary data blob for saving on Data Flower.
            # We don't have to do that here.
            # It can be pre-computed and placed on the JobSignature
            # TODO: contemplate moving auxiliary data formation to
            #       place where JobSignatures are generated and use that
            #       data for Data Flower (as it was originally intended
            #       but not implemented because saving each job's data
            #       individually to Data Flower was too slow)
            # So, here you would unpack
            # **job_kwargs
            # that you get from prioritization_claim.score_job_pairs
            # ... Until then:
            extra_data = {}
            if prioritization_claim.timezone:
                extra_data['ad_account_timezone_name'] = prioritization_claim.timezone

            with Measure.timer(f'{_measurement_name_base}.add_to_queue', tags=_measurement_tags):
                if prioritization_claim.report_age_in_days is not None:
                    Measure.histogram(f'{_measurement_name_base}.report_age', tags=_measurement_tags)(
                        prioritization_claim.report_age_in_days
                    )
                add_to_queue(prioritization_claim.job_id, score, **extra_data)

            # This time includes the time consumer of this generator wastes
            # between reads from us. Good way to measure how quickly we are
            # consumed (what pauses we have between each consumption)
            with Measure.timer(f'{_measurement_name_base}.yield_result', tags=_measurement_tags):
                yield prioritization_claim

            _before_next_prioritized = time.time()
