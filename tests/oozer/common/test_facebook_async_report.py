from unittest.mock import Mock

from oozer.common.facebook_async_report import FacebookAsyncReportStatus


def test_backoff_interval():
    mock_report = Mock()
    status = FacebookAsyncReportStatus(mock_report)

    result = []
    for i in range(7):
        result.append(status.backoff_interval)
        status.refresh()

    assert [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 16.0] == result
