import functools
import math
import time
from typing import Any

from facebook_business.exceptions import FacebookError

from common.bugsnag import BugSnagContextData, SEVERITY_WARNING
from common.enums.failure_bucket import FailureBucket
from common.tokens import PlatformTokenManager
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import FacebookApiErrorInspector


def _report_failure(job_scope: JobScope, start_time: float, exc: Exception):
    """
    Report task stats when task fails
    """
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)

    token_manager = PlatformTokenManager.from_job_scope(job_scope)
    token = job_scope.token

    if isinstance(exc, FacebookError):
        BugSnagContextData.notify(exc, severity=SEVERITY_WARNING, job_scope=job_scope)

        inspector = FacebookApiErrorInspector(exc)
        failure_status, failure_bucket = inspector.get_status_and_bucket()
        report_job_status_task.delay(failure_status, job_scope)
        token_manager.report_usage_per_failure_bucket(token, failure_bucket)
    else:
        BugSnagContextData.notify(exc, job_scope=job_scope)
        report_job_status_task.delay(ExternalPlatformJobStatus.GenericError, job_scope)
        token_manager.report_usage_per_failure_bucket(token, FailureBucket.Other)


def _report_success(job_scope: JobScope, start_time: float, retval: Any):
    """
    Report task stats when successful.
    """
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)

    if isinstance(retval, int):
        job_scope.datapoint_count = retval

    report_job_status_task.delay(ExternalPlatformJobStatus.Done, job_scope)


def reported_task(func):
    """Report task stats."""
    @functools.wraps(func)
    def wrapper(job_scope: JobScope, *args: Any, **kwargs: Any):
        start_time = time.time()
        report_job_status_task.delay(ExternalPlatformJobStatus.Start, job_scope)
        try:
            retval = func(job_scope, *args, **kwargs)
            _report_success(job_scope, start_time, retval)
        except Exception as exc:
            _report_failure(job_scope, start_time, exc)
            raise

    return wrapper
