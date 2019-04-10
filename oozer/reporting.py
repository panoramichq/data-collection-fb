import functools
import logging
import math
import time
from typing import Any, Callable

from common.enums.failure_bucket import FailureBucket
from common.error_inspector import ErrorInspector, ErrorTypesReport
from common.tokens import PlatformTokenManager
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import FacebookApiErrorInspector
from oozer.common.errors import CollectionError, TaskOutsideSweepException
from oozer.common.sweep_status_tracker import SweepStatusTracker

logger = logging.getLogger(__name__)


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


def _report_success(job_scope: JobScope, start_time: float, retval: Any):
    """Report task stats when successful."""
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)

    if isinstance(retval, int):
        job_scope.datapoint_count = retval

    report_job_status_task.delay(ExternalPlatformJobStatus.Done, job_scope)
    SweepStatusTracker(job_scope.sweep_id).report_status(FailureBucket.Success)


def reported_task(func: Callable) -> Callable:
    """Report task stats."""

    @functools.wraps(func)
    def wrapper(job_scope: JobScope, *args: Any, **kwargs: Any):
        start_time = time.time()
        report_job_status_task.delay(ExternalPlatformJobStatus.Start, job_scope)
        try:
            retval = func(job_scope, *args, **kwargs)
            _report_success(job_scope, start_time, retval)
        except TaskOutsideSweepException as e:
            logger.info(f'{e.job_scope} skipped because sweep {e.job_scope.sweep_id} is done')
            ErrorInspector.send_measurement_error(ErrorTypesReport.SWEEP_ALREADY_ENDED, job_scope.ad_account_id)
        except CollectionError as e:
            _report_failure(job_scope, start_time, e.inner, partial_datapoint_count=e.partial_datapoint_count)
        except Exception as e:
            _report_failure(job_scope, start_time, e)

    return wrapper
