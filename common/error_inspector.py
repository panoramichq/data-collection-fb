import logging
import traceback
from typing import Any, Dict

from gevent import Timeout
from facebook_business.exceptions import FacebookRequestError
from pynamodb.exceptions import UpdateError, GetError, PutError, QueryError

from common.bugsnag import SEVERITY_ERROR, BugSnagContextData, SEVERITY_WARNING
from common.enums.failure_bucket import FailureBucket
from common.measurement import Measure
from common.util import redact_access_token_from_str
from config.bugsnag import API_KEY
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
    INACCESSIBLE_OBJECT = 'inaccessible-object'


MAPPING_FACEBOOK_ERRORS = {
    FailureBucket.TooLarge: ErrorTypesReport.TOO_MUCH_DATA,
    FailureBucket.Throttling: ErrorTypesReport.THROTTLING_ERROR,
    FailureBucket.AdAccountThrottling: ErrorTypesReport.ACCOUNT_RATE_LIMIT,
    FailureBucket.ApplicationThrottling: ErrorTypesReport.APP_RATE_LIMIT,
    FailureBucket.UserThrottling: ErrorTypesReport.USER_RATE_LIMIT,
    FailureBucket.InaccessibleObject: ErrorTypesReport.INACCESSIBLE_OBJECT,
}


class ErrorInspector:
    @staticmethod
    def is_dynamo_throughput_error(exc: Exception) -> bool:
        return 'ProvisionedThroughputExceededException' in str(exc) and (
            isinstance(exc, UpdateError)
            or isinstance(exc, GetError)
            or isinstance(exc, PutError)
            or isinstance(exc, QueryError)
        )

    @staticmethod
    def send_measurement_error(error_type: str, ad_account_id: str):
        Measure.counter(__name__ + '.errors', {'error_type': error_type, 'ad_account_id': ad_account_id}).increment()

    @staticmethod
    def _get_trackback_exception(exception: Exception) -> str:
        return ''.join(
            traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
        ).replace('\n', '\\n')

    @staticmethod
    def inspect(exc: Exception, ad_account_id: str = None, extra_data: Dict[str, Any] = None):
        error_type = ErrorTypesReport.UNKNOWN
        report_to_bugsnag = True

        severity = SEVERITY_ERROR
        if isinstance(exc, FacebookRequestError):
            report_to_bugsnag = False
            _, failure_bucket = FacebookApiErrorInspector(exc).get_status_and_bucket()
            error_type = MAPPING_FACEBOOK_ERRORS.get(failure_bucket, ErrorTypesReport.UNKNOWN)

        elif isinstance(exc, Timeout) or isinstance(exc, TimeoutError):
            error_type = ErrorTypesReport.TIMEOUT
            report_to_bugsnag = False

        elif ErrorInspector.is_dynamo_throughput_error(exc):
            error_type = ErrorTypesReport.DYNAMO_PROVISIONING
            report_to_bugsnag = False

        final_extra_data = {'error_type': error_type, **(extra_data or {})}

        # Notify team when page not accessible
        if error_type == ErrorTypesReport.INACCESSIBLE_OBJECT:
            severity = SEVERITY_WARNING
            report_to_bugsnag = True

        if report_to_bugsnag and API_KEY:
            BugSnagContextData.notify(exc, severity=severity, **final_extra_data)

        logger.warning(
            f'[error-inspector] We encountered exception in tasks with following extra_data ->  {final_extra_data}\n'
            + redact_access_token_from_str(ErrorInspector._get_trackback_exception(exc))
        )

        ErrorInspector.send_measurement_error(error_type, ad_account_id)
