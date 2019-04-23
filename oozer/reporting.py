import functools
import logging
import math
import time
from typing import Any, Callable

import gevent

from common.enums.failure_bucket import FailureBucket
from common.error_inspector import ErrorInspector, ErrorTypesReport
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import FacebookApiErrorInspector
from oozer.common.errors import CollectionError, TaskOutsideSweepException
from oozer.common.sweep_status_tracker import SweepStatusTracker

logger = logging.getLogger(__name__)

PROGRESS_REPORTING_INTERVAL = 5 * 60


class TaskProgressReporter:

    job_scope: JobScope
    should_stop: bool = False

    def __init__(self, job_scope: JobScope):
        self.job_scope = job_scope

    def stop(self):
        self.should_stop = True

    def __call__(self, *args, **kwargs):
        report_job_status_task(ExternalPlatformJobStatus.Start, self.job_scope)
        interval = PROGRESS_REPORTING_INTERVAL
        while not self.should_stop:
            gevent.sleep(interval)
            if self.should_stop:
                return
            before = time.time()
            # Purposefully not using delay here
            report_job_status_task(ExternalPlatformJobStatus.DataFetched, self.job_scope)
            # Correct for interval "drift"
            interval = PROGRESS_REPORTING_INTERVAL - (time.time() - before)


def _report_failure(job_scope: JobScope, start_time: float, exc: Exception, **kwargs: Any):
    """Report task stats when task fails."""
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)
    job_scope.datapoint_count = kwargs.get('partial_datapoint_count')

    ErrorInspector.inspect(exc, job_scope.ad_account_id, {'job_scope': job_scope})

    token = job_scope.token
    failure_description = FacebookApiErrorInspector(exc).get_status_and_bucket()
    if failure_description:
        failure_status, failure_bucket = failure_description
    else:
        failure_status = ExternalPlatformJobStatus.GenericError
        failure_bucket = FailureBucket.Other

    report_job_status_task.delay(failure_status, job_scope)
    PlatformTokenManager.from_job_scope(job_scope).report_usage_per_failure_bucket(token, failure_bucket)
    SweepStatusTracker(job_scope.sweep_id).report_status(failure_bucket)
    _send_measurement_task_runtime(job_scope, failure_bucket)


def _report_success(job_scope: JobScope, start_time: float, ret_value: Any):
    """Report task stats when successful."""
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)

    if isinstance(ret_value, int):
        job_scope.datapoint_count = ret_value

    report_job_status_task.delay(ExternalPlatformJobStatus.Done, job_scope)
    SweepStatusTracker(job_scope.sweep_id).report_status(FailureBucket.Success)
    _send_measurement_task_runtime(job_scope, FailureBucket.Success)


def _report_start(job_scope: JobScope) -> TaskProgressReporter:
    """Report task started."""
    SweepStatusTracker(job_scope.sweep_id).report_status(FailureBucket.WorkingOnIt)
    reporter = TaskProgressReporter(job_scope)
    gevent.spawn(reporter)
    return reporter


def _send_measurement_task_runtime(job_scope: JobScope, bucket: int):
    _measurement_base_name = f'{__name__}.report_tasks_outcome'
    _measurement_tags = {
        'ad_account_id': job_scope.ad_account_id,
        'sweep_id': job_scope.sweep_id,
        'report_type': job_scope.report_type,
        'report_variant': job_scope.report_variant,
        'bucket': bucket,
        'job_type': job_scope.job_type,
    }
    if job_scope.datapoint_count and job_scope.datapoint_count > 0:
        Measure.counter(f'{_measurement_base_name}.data_points', tags=_measurement_tags).increment(
            job_scope.datapoint_count
        )

    Measure.gauge(f'{_measurement_base_name}.running_time', tags=_measurement_tags)(job_scope.running_time)


def reported_task(func: Callable) -> Callable:
    """Report task stats."""

    @functools.wraps(func)
    def wrapper(job_scope: JobScope, *args: Any, **kwargs: Any):
        start_time = time.time()
        progress_reporter = _report_start(job_scope)
        try:
            ret_value = func(job_scope, *args, **kwargs)
            progress_reporter.stop()
            _report_success(job_scope, start_time, ret_value)
        except TaskOutsideSweepException as e:
            logger.info(f'{e.job_scope} skipped because sweep {e.job_scope.sweep_id} is done')
            ErrorInspector.send_measurement_error(ErrorTypesReport.SWEEP_ALREADY_ENDED, job_scope.ad_account_id)
        except CollectionError as e:
            _report_failure(job_scope, start_time, e.inner, partial_datapoint_count=e.partial_datapoint_count)
            progress_reporter.stop()
        except Exception as e:
            _report_failure(job_scope, start_time, e)
            progress_reporter.stop()

    return wrapper
