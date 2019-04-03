import pytest

from unittest.mock import patch, Mock

from oozer.common.errors import TaskOutsideSweepException
from oozer.common.sweep_running_flag import sweep_running


@patch('oozer.common.sweep_running_flag.SweepRunningFlag.is_set')
def test_sweep_running_flag_not_set(mock_is_set):
    mock_is_set.return_value = False

    @sweep_running
    def test_func(_):
        pytest.fail('Expected TaskOutsideSweepException to be raised')

    with pytest.raises(TaskOutsideSweepException):
        test_func(Mock(sweep_id='test-sweep-id'))


@patch('oozer.common.sweep_running_flag.SweepRunningFlag.is_set')
def test_sweep_running_flag_is_set(mock_is_set):
    mock_is_set.return_value = True

    @sweep_running
    def test_func(_):
        return 100

    assert 100 == test_func(Mock(sweep_id='test-sweep-id'))
