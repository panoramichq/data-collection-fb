import logging
from typing import Any, Dict

from gevent import Timeout
from facebook_business.exceptions import FacebookError
from pynamodb.exceptions import UpdateError, GetError, PutError, QueryError

from common.bugsnag import SEVERITY_ERROR, BugSnagContextData
from common.enums.failure_bucket import FailureBucket
from common.measurement import Measure
from oozer.common.facebook_api import FacebookApiErrorInspector

logger = logging.getLogger(__name__)


class ErrorTypesReport:

    UNKNOWN = 'unknown'
    SWEEP_ALREADY_ENDED = 'sweep-ended'
    TIMEOUT = 'timeout'
    APP_RATE_LIMIT = 'app-rate-limit'
    ACCOUNT_RATE_LIMIT = 'account-rate-limit'
    USER_RATE_LIMIT = 'user-rate-limit'
    THROTTLING_ERROR = 'throttling-error'
    TOO_MUCH_DATA = 'too-much-data'
    DYNAMO_PROVISIONING = 'dynamo-provisioning'


MAPPING_FACEBOOK_ERRORS = {
    FailureBucket.TooLarge: ErrorTypesReport.TOO_MUCH_DATA,
    FailureBucket.Throttling: ErrorTypesReport.THROTTLING_ERROR,
    FailureBucket.AdAccountThrottling: ErrorTypesReport.ACCOUNT_RATE_LIMIT,
    FailureBucket.ApplicationThrottling: ErrorTypesReport.APP_RATE_LIMIT,
    FailureBucket.UserThrottling: ErrorTypesReport.USER_RATE_LIMIT,
}


class ErrorInspector:
    @staticmethod
    def _is_dynamo_error(exc: Exception) -> bool:
        return (
            isinstance(exc, UpdateError)
            or isinstance(exc, GetError)
            or isinstance(exc, PutError)
            or isinstance(exc, QueryError)
        )

    @staticmethod
    def send_measurement_error(error_type: str, ad_account_id: str):
        Measure.counter(__name__ + '.errors', {'ad_account_id': ad_account_id, 'error_type': error_type}).increment()

    @staticmethod
    def inspect(exc: Exception, ad_account_id: str = None, extra_data: Dict[str, Any] = None):
        error_type = ErrorTypesReport.UNKNOWN
        report_to_bugsnag = True

        severity = SEVERITY_ERROR
        if isinstance(exc, FacebookError):
            report_to_bugsnag = False
            fb_error_inspector = FacebookApiErrorInspector(exc)

            _, failure_bucket = fb_error_inspector.get_status_and_bucket()
            error_type = MAPPING_FACEBOOK_ERRORS.get(failure_bucket, ErrorTypesReport.UNKNOWN)

        elif isinstance(exc, Timeout) or isinstance(exc, TimeoutError):
            error_type = ErrorTypesReport.TIMEOUT
            report_to_bugsnag = False

        elif ErrorInspector._is_dynamo_error(exc):
            error_type = ErrorTypesReport.DYNAMO_PROVISIONING
            report_to_bugsnag = False

        if report_to_bugsnag:
            BugSnagContextData.notify(exc, severity=severity, **extra_data)

        logger.warning(
            f'[error-inspector] We encountered exception in tasks with following extra_data ->  {extra_data}'
        )
        logger.warning(str(exc))

        ErrorInspector.send_measurement_error(error_type, ad_account_id)
