import gevent

from unittest.mock import sentinel, patch, call

from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.task_progress_reporter import TaskProgressReporter


@patch('oozer.common.task_progress_reporter.report_job_status_task')
@patch('oozer.common.task_progress_reporter.PROGRESS_REPORTING_INTERVAL', new=0.1)
def test_reporter(mock_report_job_status):
    reporter = TaskProgressReporter(sentinel.mock_scope)

    gevent.spawn(reporter)
    gevent.sleep(0.3)
    reporter.stop()

    mock_report_job_status.assert_has_calls([
        call(ExternalPlatformJobStatus.Start, sentinel.mock_scope),
        call(ExternalPlatformJobStatus.DataFetched, sentinel.mock_scope),
    ])
