import functools
import logging
import math
import time
from typing import Any, Callable

from facebook_business.exceptions import FacebookError
from gevent import Timeout
from gevent.exceptions import ConcurrentObjectUseError
from pynamodb.exceptions import UpdateError, GetError, PutError, QueryError

from common.bugsnag import BugSnagContextData, SEVERITY_ERROR
from common.enums.failure_bucket import FailureBucket
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import FacebookApiErrorInspector
from oozer.common.errors import CollectionError, TaskOutsideSweepException

logger = logging.getLogger(__name__)


TOO_MANY_OPEN_FILES_TEXT = 'Too many open files'


class ErrorTypesReport:

    UNKNOWN = 'unknown'
    SWEEP_ALREADY_ENDED = 'sweep-ended'
    TIMEOUT = 'timeout'
    CONCURRENCY_ISSUE = 'concurrency-issue'
    RATE_LIMIT = 'rate-limit'
    TOO_MUCH_DATA = 'too-much-data'
    DYNAMO_PROVISIONING = 'dynamo-provisioning'
    CACHE_TOO_MANY_FILES = 'cache-too-many-files'

    _MAPPING_TO_FAILURE_BUCKETS = {
        FailureBucket.Other: UNKNOWN,
        FailureBucket.Throttling: RATE_LIMIT,
        FailureBucket.TooLarge: TOO_MUCH_DATA,
    }

    @staticmethod
    def map_failure_bucket_to_type(failure_bucket: int):
        return ErrorTypesReport._MAPPING_TO_FAILURE_BUCKETS.get(failure_bucket, ErrorTypesReport.UNKNOWN)


def _send_measurement_error(error_type: str, ad_account_id: str):
    Measure.counter(__name__ + '.errors', {'ad_account_id': ad_account_id, 'error_type': error_type}).increment()


def _detect_dynamo_error(exc: Exception):
    return (
        isinstance(exc, UpdateError)
        or isinstance(exc, GetError)
        or isinstance(exc, PutError)
        or isinstance(exc, QueryError)
    )


def _report_failure(job_scope: JobScope, start_time: float, exc: Exception, **kwargs: Any):
    """Report task stats when task fails."""
    error_type = ErrorTypesReport.UNKNOWN
    end_time = time.time()
    report_to_bugsnag = True
    job_scope.running_time = math.ceil(end_time - start_time)
    job_scope.datapoint_count = kwargs.get('partial_datapoint_count')

    token_manager = PlatformTokenManager.from_job_scope(job_scope)
    token = job_scope.token

    severity = SEVERITY_ERROR
    failure_bucket = FailureBucket.Other
    failure_status = ExternalPlatformJobStatus.GenericError

    # let's make sure we dont report 'useless' errors to bugsnag
    if isinstance(exc, FacebookError):
        report_to_bugsnag = False
        failure_status, failure_bucket = FacebookApiErrorInspector(exc).get_status_and_bucket()
        error_type = ErrorTypesReport.map_failure_bucket_to_type(failure_bucket)
    elif isinstance(exc, Timeout) or isinstance(exc, TimeoutError):
        error_type = ErrorTypesReport.TIMEOUT
        report_to_bugsnag = False
    elif isinstance(exc, ConcurrentObjectUseError):
        error_type = ErrorTypesReport.CONCURRENCY_ISSUE
        report_to_bugsnag = False
    elif _detect_dynamo_error(exc):
        error_type = ErrorTypesReport.DYNAMO_PROVISIONING
        report_to_bugsnag = False
    elif TOO_MANY_OPEN_FILES_TEXT in str(exc):
        error_type = ErrorTypesReport.CACHE_TOO_MANY_FILES
        report_to_bugsnag = False

    if report_to_bugsnag:
        BugSnagContextData.notify(exc, severity=severity, job_scope=job_scope)
    else:
        logger.warning(f'We encountered exception in tasks with following job_scope ->  {job_scope}')

    _send_measurement_error(job_scope.ad_account_id, error_type)
    report_job_status_task.delay(failure_status, job_scope)
    token_manager.report_usage_per_failure_bucket(token, failure_bucket)


def _report_success(job_scope: JobScope, start_time: float, retval: Any):
    """Report task stats when successful."""
    end_time = time.time()
    job_scope.running_time = math.ceil(end_time - start_time)

    if isinstance(retval, int):
        job_scope.datapoint_count = retval

    report_job_status_task.delay(ExternalPlatformJobStatus.Done, job_scope)


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
            _send_measurement_error(job_scope.ad_account_id, ErrorTypesReport.SWEEP_ALREADY_ENDED)
        except CollectionError as e:
            _report_failure(job_scope, start_time, e.inner, partial_datapoint_count=e.partial_datapoint_count)
            raise
        except Exception as e:
            _report_failure(job_scope, start_time, e)
            raise

    return wrapper
