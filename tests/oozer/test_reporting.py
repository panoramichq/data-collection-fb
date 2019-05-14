from unittest.mock import patch, call, Mock

from facebook_business.exceptions import FacebookError, FacebookRequestError

from common.bugsnag import SEVERITY_ERROR
from common.enums.failure_bucket import FailureBucket
from common.error_inspector import ErrorTypesReport
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.reporting import reported_task


@patch('oozer.reporting.report_job_status_task')
def test_reported_task_on_success(mock_report):
    mock_job_scope = Mock()

    @reported_task
    def test_task(*_, **__):
        return 10

    test_task(mock_job_scope)

    assert mock_job_scope.datapoint_count == 10
    assert mock_job_scope.running_time is not None

    assert mock_report.delay.call_args_list == [call(ExternalPlatformJobStatus.Done, mock_job_scope)]


@patch('oozer.reporting.PlatformTokenManager.from_job_scope')
@patch('oozer.reporting.report_job_status_task')
@patch('common.error_inspector.BugSnagContextData.notify')
@patch('oozer.reporting.FacebookApiErrorInspector.get_status_and_bucket')
@patch('common.error_inspector.API_KEY', 'something')
def test_reported_task_on_failure_facebook_error(
    mock_get_status_and_bucket, mock_notify, mock_report, mock_from_job_scope
):
    exc = FacebookRequestError('test', {}, 404, [], '')
    mock_job_scope = Mock(token='token')
    mock_get_status_and_bucket.return_value = (
        ExternalPlatformJobStatus.UserThrottlingError,
        FailureBucket.UserThrottling,
    )

    @reported_task
    def test_task(*_, **__):
        raise exc

    test_task(mock_job_scope)

    assert mock_job_scope.running_time is not None
    assert mock_report.delay.call_args_list == [call(ExternalPlatformJobStatus.UserThrottlingError, mock_job_scope)]

    assert not mock_notify.called
    mock_from_job_scope.return_value.report_usage_per_failure_bucket.assert_called_once_with(
        'token', FailureBucket.UserThrottling
    )


@patch('oozer.reporting.PlatformTokenManager.from_job_scope')
@patch('oozer.reporting.report_job_status_task')
@patch('common.error_inspector.BugSnagContextData.notify')
@patch('common.error_inspector.API_KEY', 'something')
def test_reported_task_on_failure_generic_error(mock_notify, mock_report, mock_from_job_scope):
    exc = Exception('test')
    mock_job_scope = Mock(token='token')

    @reported_task
    def test_task(*_, **__):
        raise exc

    test_task(mock_job_scope)

    assert mock_job_scope.running_time is not None
    assert mock_report.delay.call_args_list == [call(ExternalPlatformJobStatus.GenericError, mock_job_scope)]

    mock_notify.assert_called_once_with(
        exc, job_scope=mock_job_scope, severity=SEVERITY_ERROR, error_type=ErrorTypesReport.UNKNOWN
    )
    mock_from_job_scope.return_value.report_usage_per_failure_bucket.assert_called_once_with(
        'token', FailureBucket.Other
    )
