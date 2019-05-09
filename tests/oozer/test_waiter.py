import time
from datetime import timedelta
from unittest.mock import Mock

from oozer.waiter import TaskWaiter


def test_waiter_should_terminate_when_processed_more_than_90percent():
    mock_tracker = Mock()
    mock_tracker.get_pulse.return_value = Mock(Total=95)
    with TaskWaiter('sweep-id', mock_tracker, 100, 0) as waiter:
        assert waiter.should_terminate()


def test_waiter_should_terminate_when_current_time_more_than_stop_time():
    mock_tracker = Mock()
    mock_tracker.get_pulse.return_value = Mock(Total=0)
    stop_time = time.time()
    with TaskWaiter('sweep-id', mock_tracker, 100, stop_time) as waiter:
        assert waiter.should_terminate()


def test_waiter_should_not_terminate():
    mock_tracker = Mock()
    mock_tracker.get_pulse.return_value = Mock(Total=0)
    stop_time = time.time() + timedelta(hours=1).total_seconds()
    with TaskWaiter('sweep-id', mock_tracker, 100, stop_time) as waiter:
        assert not waiter.should_terminate()
