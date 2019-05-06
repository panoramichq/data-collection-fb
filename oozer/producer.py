import logging
from typing import Generator, Tuple

from common.celeryapp import CeleryTask
from common.error_inspector import ErrorInspector
from common.id_tools import parse_id
from oozer.common.errors import InvalidJobScopeException
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from oozer.inventory import resolve_job_scope_to_celery_task

logger = logging.getLogger(__name__)


class TaskProducer:
    def __init__(self, sweep_id: str):
        self.sweep_id = sweep_id
        self.queue = SortedJobsQueue(sweep_id)

    def get_ad_account_count(self) -> int:
        """Number of unique ad accounts we scheduled tasks for."""
        return self.queue.get_ad_accounts_count()

    def get_task_count(self) -> int:
        """Number of tasks we scheduled."""
        return self.queue.get_queue_length()

    def iter_tasks(self) -> Generator[Tuple[CeleryTask, JobScope, JobContext, int], None, None]:
        """Read persisted jobs and pass-through context objects for inspection"""
        with self.queue.JobsReader() as jobs_iter:
            for job_id, job_scope_additional_data, score in jobs_iter:

                job_id_parts = parse_id(job_id)
                job_scope = JobScope(job_scope_additional_data, job_id_parts, sweep_id=self.sweep_id, score=score)

            try:
                celery_task = resolve_job_scope_to_celery_task(job_scope)
                # TODO: Decide what to do with this.
                # Was designed for massive hash collection and such,
                # but cannot have too much data in there because we pickle it and put in on Redis
                job_context = JobContext()
                yield celery_task, job_scope, job_context, score
                logger.info(f"#{self.sweep_id}: Scheduling job_id {job_id} with score {score}.")
            except InvalidJobScopeException as e:
                ErrorInspector.inspect(
                    e, job_scope.ad_account_id, {'sweep_id': job_scope.sweep_id, 'job_id': job_scope.job_id}
                )
