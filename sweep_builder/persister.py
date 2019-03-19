import logging
import time

from collections import defaultdict
from typing import Generator, Iterable

from common.measurement import Measure
from oozer.common.expecations_store import JobExpectationsWriter
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim

logger = logging.getLogger(__name__)


def should_persist(job_score: int) -> bool:
    """Determine whether job with score should be persisted."""
    return job_score > JobGateKeeper.JOB_NOT_PASSED_SCORE


def iter_persist_prioritized(
    sweep_id: str, prioritized_iter: Iterable[PrioritizationClaim]
) -> Generator[PrioritizationClaim, None, None]:
    """Persist prioritized jobs and pass-through context objects for inspection."""
    with SortedJobsQueue(sweep_id).JobsWriter() as add_to_queue, JobExpectationsWriter(
        sweep_id, cache_max_size=200000
    ) as expectation_add:

        _measurement_name_base = f"{__name__}.{iter_persist_prioritized.__name__}"

        _before_next_prioritized = time.time()
        skipped_jobs = defaultdict(int)
        for prioritization_claim in prioritized_iter:
            ad_account_id = prioritization_claim.ad_account_id
            entity_type = prioritization_claim.entity_type
            job_id_effective = prioritization_claim.selected_job_id
            score = prioritization_claim.score

            _measurement_tags = {'ad_account_id': ad_account_id, 'entity_type': entity_type, 'sweep_id': sweep_id}

            Measure.timing(f'{_measurement_name_base}.next_prioritized', tags=_measurement_tags)(
                (time.time() - _before_next_prioritized) * 1000
            )

            if not should_persist(score):
                logger.info(f'Not persisting job {job_id_effective} due to low score: {score}')
                skipped_jobs[ad_account_id] += 1
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
                add_to_queue(job_id_effective, score, **extra_data)

            # This is our cheap way of ensuring that we are dealing
            # with platform-bound job that we need to report our expectations for
            if prioritization_claim.is_subject_to_expectation_publication:
                # TODO: contemplate parsing these instead and making sure they are norm vs eff
                # at this point all this checks is that we have more than one job_id scheduled
                if prioritization_claim.normative_job_id != job_id_effective:
                    with Measure.timer(f'{_measurement_name_base}.expectation_add', tags=_measurement_tags):
                        expectation_add(job_id_effective, ad_account_id, prioritization_claim.entity_id)

            # This time includes the time consumer of this generator wastes
            # between reads from us. Good way to measure how quickly we are
            # consumed (what pauses we have between each consumption)
            with Measure.timer(f'{_measurement_name_base}.yield_result', tags=_measurement_tags):
                yield prioritization_claim

            _before_next_prioritized = time.time()

        if skipped_jobs:
            for (ad_account_id, count) in skipped_jobs.items():
                Measure.gauge(
                    f'{_measurement_name_base}.gatekeeper_stop_jobs',
                    tags={'sweep_id': sweep_id, 'ad_account_id': ad_account_id},
                )(count)
