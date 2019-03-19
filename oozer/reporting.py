import functools
import math
import time
from typing import Any

from facebook_business.exceptions import FacebookError

from common.bugsnag import BugSnagContextData, SEVERITY_WARNING, SEVERITY_ERROR
from common.enums.failure_bucket import FailureBucket
from common.tokens import PlatformTokenManager
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import FacebookApiErrorInspector
from oozer.common.errors import CollectionError


def _report_failure(job_scope: JobScope, start_time: float, exc: Exception, **kwargs: Any):
    """Report task stats when task fails."""
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)
    job_scope.datapoint_count = kwargs.get('partial_datapoint_count')

    token_manager = PlatformTokenManager.from_job_scope(job_scope)
    token = job_scope.token

    severity = SEVERITY_ERROR
    failure_bucket = FailureBucket.Other
    failure_status = ExternalPlatformJobStatus.GenericError

    if isinstance(exc, FacebookError):
        severity = SEVERITY_WARNING
        failure_status, failure_bucket = FacebookApiErrorInspector(exc).get_status_and_bucket()

    BugSnagContextData.notify(exc, severity=severity, job_scope=job_scope)
    report_job_status_task.delay(failure_status, job_scope)
    token_manager.report_usage_per_failure_bucket(token, failure_bucket)


def _report_success(job_scope: JobScope, start_time: float, retval: Any):
    """Report task stats when successful."""
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
        except CollectionError as e:
            _report_failure(job_scope, start_time, e.inner, partial_datapoint_count=e.partial_datapoint_count)
            raise
        except Exception as e:
            _report_failure(job_scope, start_time, e)
            raise

    return wrapper
