import pytest

from unittest.mock import patch, call, Mock

from facebook_business.exceptions import FacebookError

from common.bugsnag import SEVERITY_WARNING, SEVERITY_ERROR
from common.enums.failure_bucket import FailureBucket
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.reporting import reported_task


@patch('oozer.tasks.report_job_status_task')
def test_reported_task_on_success(mock_report):
    mock_job_scope = Mock()

    @reported_task
    def test_task(*_, **__):
        return 10

    test_task(mock_job_scope)

    assert mock_job_scope.datapoint_count == 10
    assert mock_job_scope.running_time is not None

    assert mock_report.delay.call_args_list == [
        call(ExternalPlatformJobStatus.Start, mock_job_scope),
        call(ExternalPlatformJobStatus.Done, mock_job_scope),
    ]


@patch('oozer.tasks.PlatformTokenManager.from_job_scope')
@patch('oozer.tasks.report_job_status_task')
@patch('oozer.tasks.BugSnagContextData.notify')
@patch('oozer.tasks.FacebookApiErrorInspector.get_status_and_bucket')
def test_reported_task_on_failure_facebook_error(mock_get_status_and_bucket, mock_notify, mock_report, mock_from_job_scope):
    exc = FacebookError('test')
    mock_job_scope = Mock(token='token')
    mock_get_status_and_bucket.return_value = (
        ExternalPlatformJobStatus.ThrottlingError,
        FailureBucket.Throttling,
    )

    @reported_task
    def test_task(*_, **__):
        raise exc

    try:
        test_task(mock_job_scope)
        pytest.fail('Exception expected to be thrown')
    except FacebookError:
        pass

    assert mock_job_scope.running_time is not None
    assert mock_report.delay.call_args_list == [
        call(ExternalPlatformJobStatus.Start, mock_job_scope),
        call(ExternalPlatformJobStatus.ThrottlingError, mock_job_scope),
    ]

    mock_notify.assert_called_once_with(exc, job_scope=mock_job_scope, severity=SEVERITY_WARNING)
    mock_from_job_scope.return_value.report_usage_per_failure_bucket.assert_called_once_with(
        'token',
        FailureBucket.Throttling
    )


@patch('oozer.tasks.PlatformTokenManager.from_job_scope')
@patch('oozer.tasks.report_job_status_task')
@patch('oozer.tasks.BugSnagContextData.notify')
def test_reported_task_on_failure_generic_error(mock_notify, mock_report, mock_from_job_scope):
    exc = Exception('test')
    mock_job_scope = Mock(token='token')

    @reported_task
    def test_task(*_, **__):
        raise exc

    try:
        test_task(mock_job_scope)
        pytest.fail('Exception expected to be thrown')
    except Exception:
        pass

    assert mock_job_scope.running_time is not None
    assert mock_report.delay.call_args_list == [
        call(ExternalPlatformJobStatus.Start, mock_job_scope),
        call(ExternalPlatformJobStatus.GenericError, mock_job_scope),
    ]

    mock_notify.assert_called_once_with(exc, job_scope=mock_job_scope, severity=SEVERITY_ERROR)
    mock_from_job_scope.return_value.report_usage_per_failure_bucket.assert_called_once_with(
        'token',
        FailureBucket.Other
    )
