import logging
import time
from collections import defaultdict

from typing import Generator, Iterable

from common.enums.jobtype import detect_job_type
from common.measurement import Measure
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper, JobGateKeeperCache
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim

logger = logging.getLogger(__name__)
CUTOFF_SCORE = max(JobGateKeeper.JOB_NOT_PASSED_SCORE, JobGateKeeperCache.JOB_NOT_PASSED_SCORE)


class JobCounter:

    _COUNTER_STEP = 100
    _SCORED_JOBS_NAME = f'{__name__}.scored_jobs'
    _GATEKEEPER_JOBS_NAME = f'{__name__}.gatekeeper_skipped_jobs'
    _GATEKEEPER_CACHE_JOBS_NAME = f'{__name__}.gatekeeper_cache_skipped_jobs'
    _PASSED_JOBS_NAME = f'{__name__}.passed_jobs'
    _COUNTER_NAME_MAP = {
        JobGateKeeper.JOB_NOT_PASSED_SCORE: _GATEKEEPER_JOBS_NAME,
        JobGateKeeperCache.JOB_NOT_PASSED_SCORE: _GATEKEEPER_CACHE_JOBS_NAME,
    }

    def __init__(self, sweep_id: str):
        self.sweep_id = sweep_id
        self.counters = defaultdict(int)
        self.counter_total = defaultdict(int)
        self.tags = {'sweep_id': self.sweep_id}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._flush()

    def _get_counter_name(self, claim: PrioritizationClaim) -> str:
        return self._COUNTER_NAME_MAP.get(claim.score, self._PASSED_JOBS_NAME)

    def _flush(self):
        for ((ad_account_id, job_type, report_type), counter_val) in self.counter_total.items():
            Measure.counter(
                self._SCORED_JOBS_NAME,
                tags={**self.tags, 'ad_account_id': ad_account_id, 'job_type': job_type, 'report_type': report_type},
            ).increment(counter_val % self._COUNTER_STEP)
            logger.info(
                f'[write-job-batch][{self.sweep_id}][{ad_account_id}] TotalJobCount={counter_val} '
                f'JobType="{job_type}" ReportType="{report_type}"'
            )

        for ((counter_name, ad_account_id, job_type, report_type), counter_val) in self.counters.items():
            Measure.counter(
                counter_name,
                tags={**self.tags, 'ad_account_id': ad_account_id, 'job_type': job_type, 'report_type': report_type},
            ).increment(counter_val % self._COUNTER_STEP)
            logger.info(
                f'[write-job-batch][{self.sweep_id}][{ad_account_id}] {counter_name}={counter_val} '
                f'JobType="{job_type}" ReportType="{report_type}"'
            )

    def increment(self, claim: PrioritizationClaim):
        job_type = detect_job_type(claim.report_type, claim.entity_type)
        key = (claim.ad_account_id, job_type, claim.report_type)
        self.counter_total[key] += 1
        if self.counter_total[key] % self._COUNTER_STEP == 0:
            Measure.counter(
                self._SCORED_JOBS_NAME, tags={**self.tags, 'job_type': job_type, 'report_type': claim.report_type}
            ).increment(self._COUNTER_STEP)

        counter_name = self._get_counter_name(claim)
        counter_key = (counter_name, claim.ad_account_id, job_type, claim.report_type)
        self.counters[counter_key] += 1
        if self.counters[counter_key] % self._COUNTER_STEP == 0:
            Measure.counter(
                counter_name,
                tags={
                    **self.tags,
                    'ad_account_id': claim.ad_account_id,
                    'job_type': job_type,
                    'report_type': claim.report_type,
                },
            ).increment(self._COUNTER_STEP)


def should_persist(job_score: int) -> bool:
    """Determine whether job with score should be persisted."""
    return job_score > CUTOFF_SCORE


def iter_persist_prioritized(
    sweep_id: str, prioritized_iter: Iterable[PrioritizationClaim]
) -> Generator[PrioritizationClaim, None, None]:
    """Persist prioritized jobs and pass-through context objects for inspection."""
    with SortedJobsQueue(sweep_id).JobsWriter() as add_to_queue, JobCounter(sweep_id) as counter:

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
            counter.increment(prioritization_claim)

            Measure.timing(f'{_measurement_name_base}.next_prioritized', tags=_measurement_tags)(
                (time.time() - _before_next_prioritized) * 1000, sample_rate=0.01
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
