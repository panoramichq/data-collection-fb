# must be first, as it does event loop patching and other "first" things
from unittest import skip

from tests.base.testcase import TestCase

from sweep_builder.tasks import sweep_builder_task
from oozer.looper import iter_tasks


class TestSweepBuilder(TestCase):

    @skip
    def test_sweep(self):

        sweep_builder_task.delay('1', True)
        tasks = [x for x in iter_tasks(1)]

        assert len(tasks) > 1


