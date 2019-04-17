from common.enums.failure_bucket import FailureBucket
from oozer.common.sweep_status_tracker import StatusCounts, Pulse
from oozer.looper import AdaptiveTaskOozer


def test_calculate_rate_no_user_throttling():
    statuses = {k: 0 for k in FailureBucket.attr_name_enum_value_map.keys()}
    statuses.update(UserThrottling=0)
    pulse = Pulse(Total=100, InProgress=10, CurrentCounts=StatusCounts(**statuses, Total=20), **statuses)
    new_rate = AdaptiveTaskOozer.calculate_rate(50, pulse)

    assert new_rate == 52.5


def test_calculate_rate_all_user_throttling():
    statuses = {k: 0 for k in FailureBucket.attr_name_enum_value_map.keys()}
    statuses.update(UserThrottling=20)
    pulse = Pulse(Total=100, InProgress=10, CurrentCounts=StatusCounts(**statuses, Total=20), **statuses)
    new_rate = AdaptiveTaskOozer.calculate_rate(50, pulse)

    assert 45 <= new_rate < 50
