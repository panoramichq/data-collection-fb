# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock, skip

from tests.base import random

from oozer.common.task_group import TaskGroup


class JobsQueueTests(TestCase):

    def test_generate_job_id(self):
        task_group = TaskGroup(number_of_shards=3)

        group_id = task_group.group_id

        all_task_ids = (
            task_group.generate_task_id(),
            task_group.generate_task_id(),
            task_group.generate_task_id(),
            task_group.generate_task_id()
        )

        # checking basic signature of the task_id and it's shard key component

        # note roll back into '1' because of number_of_shards=3
        shard_ids_should_be = ('1', '2', '0', '1')

        task_keys_should_be = ('1', '2', '3', '4')
        parts = zip(all_task_ids, shard_ids_should_be, task_keys_should_be)

        _loop_id = 0
        for task_id, shard_id_should_be, task_key_should_be in parts:
            _loop_id += 1
            print('Loop ', _loop_id)
            assert type(task_id) is tuple

            shard_key, job_key = task_id

            assert shard_key == group_id + '-' + shard_id_should_be
            assert job_key == task_key_should_be

    def test_tasks_count(self):
        task_group = TaskGroup(number_of_shards=3)

        assert task_group.get_remaining_tasks_count() == 0

        task1_id = task_group.generate_task_id()
        task2_id = task_group.generate_task_id()

        task_group.report_task_active(task1_id)
        task_group.report_task_active(task2_id)

        assert task_group.get_remaining_tasks_count() == 2

        task_group.report_task_done(task2_id)

        assert task_group.get_remaining_tasks_count() == 1

        task_group.report_task_done(task1_id)

        assert task_group.get_remaining_tasks_count() == 0

    def test_join(self):
        task_group = TaskGroup(number_of_shards=3)

        task1_id = task_group.generate_task_id()
        task_group.report_task_active(task1_id)

        join_timeout = 0.1

        with self.assertRaises(TimeoutError) as ctx:
            task_group.join(join_timeout)

        assert 'has 1 remaining tasks after timeout' in str(ctx.exception)

        task_group.report_task_done(task1_id)

        task_group.join(join_timeout)
