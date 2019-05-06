from unittest.mock import Mock

from common.enums.failure_bucket import FailureBucket
from oozer.common.sweep_status_tracker import Pulse, StatusCounts
from oozer.oozer import TaskOozer, PULSE_REVIEW_MIN_OOZED_TASKS


def test_should_terminate_false_before_review():
    mock_tracker = Mock()
    with TaskOozer('sweep-id', mock_tracker, 5) as oozer:
        assert not oozer.should_terminate()


def test_should_terminate_false_most_successful():
    mock_tracker = Mock()
    mock_tracker.get_pulse.return_value = Mock(Total=21, Success=0.5, Throttling=0)
    with TaskOozer('sweep-id', mock_tracker, 0) as oozer:
        for _ in range(PULSE_REVIEW_MIN_OOZED_TASKS):
            oozer._ooze_task(Mock(), Mock(), Mock())
        assert not oozer.should_terminate()


def test_should_terminate_true_throttling():
    mock_tracker = Mock()
    mock_tracker.get_pulse.return_value = Mock(Total=21, Success=0.5, Throttling=0.5)
    with TaskOozer('sweep-id', mock_tracker, 0) as oozer:
        for _ in range(PULSE_REVIEW_MIN_OOZED_TASKS):
            oozer._ooze_task(Mock(), Mock(), Mock())
        assert oozer.should_terminate()


def test_should_terminate_true_most_failures():
    mock_tracker = Mock()
    mock_tracker.get_pulse.return_value = Mock(Total=21, Success=0.05)
    with TaskOozer('sweep-id', mock_tracker, 0) as oozer:
        for _ in range(PULSE_REVIEW_MIN_OOZED_TASKS):
            oozer._ooze_task(Mock(), Mock(), Mock())
        assert oozer.should_terminate()


def test_calculate_rate_no_user_throttling():
    statuses = {k: 0 for k in FailureBucket.attr_name_enum_value_map.keys()}
    statuses.update(UserThrottling=0)
    pulse = Pulse(Total=100, InProgress=10, CurrentCounts=StatusCounts(**statuses, Total=20), **statuses)
    new_rate = TaskOozer.calculate_rate(50, pulse)

    assert new_rate == 52.5


def test_calculate_rate_all_user_throttling():
    statuses = {k: 0 for k in FailureBucket.attr_name_enum_value_map.keys()}
    statuses.update(UserThrottling=20)
    pulse = Pulse(Total=100, InProgress=10, CurrentCounts=StatusCounts(**statuses, Total=20), **statuses)
    new_rate = TaskOozer.calculate_rate(50, pulse)

    assert 45 <= new_rate < 50
