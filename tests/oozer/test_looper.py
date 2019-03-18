# must be first, as it does event loop patching and other "first" things
from unittest.mock import patch

import pytest
import functools
import gevent

from oozer.looper import run_sweep_looper_suggest_restart_time, Pulse
from tests.base.testcase import TestCase


def _mock_run_tasks(_, time=0):
    gevent.sleep(time)
    return 0, Pulse(Total=1, Success=1, WorkingOnIt=0, Other=0, Throttling=0, TooLarge=0)


class TestLooperTimeout(TestCase):

    @patch('oozer.looper.looper_config.OOZER_TIMEOUT', new=1)
    @patch('oozer.looper.run_tasks', new=functools.partial(_mock_run_tasks, time=10))
    @patch('oozer.looper.SweepRunningFlag')
    def test_run_tasks_does_timeout(self, _):
        with pytest.raises(gevent.Timeout):
            run_sweep_looper_suggest_restart_time('test_sweep_id')

    @patch('oozer.looper.looper_config.OOZER_TIMEOUT', new=10)
    @patch('oozer.looper.run_tasks', new=functools.partial(_mock_run_tasks, time=1))
    @patch('oozer.looper.SweepRunningFlag')
    def test_run_tasks_does_not_timeout(self, _):
        run_sweep_looper_suggest_restart_time('test_sweep_id')
