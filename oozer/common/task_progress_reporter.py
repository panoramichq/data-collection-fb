import logging
import time
from typing import Any

import gevent

from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task

PROGRESS_REPORTING_INTERVAL = 5 * 60
WARNING_THRESHOLD = 2 * 60 * 60

logger = logging.getLogger(__name__)


class TaskProgressReporter:

    job_scope: JobScope
    should_stop: bool = False

    def __init__(self, job_scope: JobScope):
        self.job_scope = job_scope

    def stop(self):
        self.should_stop = True

    def __call__(self, *_: Any, **__: Any):
        report_job_status_task(ExternalPlatformJobStatus.Start, self.job_scope)
        start_time = time.time()
        warned_already = False
        interval = PROGRESS_REPORTING_INTERVAL
        while not self.should_stop:
            gevent.sleep(interval)
            if self.should_stop:
                return
            before = time.time()
            # Purposefully not using delay here
            report_job_status_task(ExternalPlatformJobStatus.DataFetched, self.job_scope)
            # Correct for interval "drift"
            after = time.time()
            interval = PROGRESS_REPORTING_INTERVAL - (after - before)

            if not warned_already and (after - start_time) > WARNING_THRESHOLD:
                logger.warning(f'[long-running] Job {self.job_scope} being reported for long time')
                warned_already = True
