import gevent
import gevent.pool
import logging
import random
import time
import ujson as json
import uuid

from contextlib import AbstractContextManager
from typing import Tuple, Union, Optional

from common.connect.redis import get_redis


logger = logging.getLogger(__name__)


TaskID = Tuple[str, str]


class NotSet:
    pass


class TaskContext(AbstractContextManager):

    def __init__(self, task_id: Optional[TaskID]):
        self.task_id = task_id

    def report_active(self):
        if self.task_id:
            TaskGroup.report_task_active(self.task_id)

    def report_done(self):
        if self.task_id:
            TaskGroup.report_task_done(self.task_id)

    def __enter__(self):
        self.report_active()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.report_done()


class TaskGroup:
    """
    We found volume-linked anomalies in regular Celery result storage.
    With huge amount of tasks, asynchronous results never resovle.
    It's possible they are getting lost due to some que sizes...
    Instead of trying to delve through that (which would be worthy exercise)
    we are rolling our own simple task result storage.

    This is not a long term solution, but mostly a back up / troubleshooting
    bypass that we can activate on-demand when we don't know for sure why
    typical Celery result reporting does not work.

    This manual task result handling requires that workers manually report when
    they are done.

    How this works:
    - We are expecting to use Redis (server-side sharded) *Cluster*
        so the more keys we distribute the tasks over, the better
    - We are using a pool of Redis Hash structures.
        Outer keys are Group ID derived, pre-sharded keys communally representing
        entire group of tasks.
        Inner keys (ones that directly hold the value) are IDs of the actual tasks.
    - Values of the keys represent some level of activity by the task
        but is not particularly important to the implementation.
        Managed by this class internally.
        Key-Key-Value is removed when task is reported as done.
    """

    _key_suffix = '_tasks_group'

    def __init__(self, group_id=None, number_of_shards=10):
        self._last_task_key = 0
        self.number_of_shards = number_of_shards
        self.group_id = group_id or str(uuid.uuid4())

    def __repr__(self):
        return f'<TaskGroup {self.group_id}:{self.number_of_shards}>'

    def _get_next_task_key_int(self):
        """
        :return: Monotonically-increasing Int
        :rtype: int
        """
        self._last_task_key = task_key = self._last_task_key + 1
        return task_key

    def _generate_shard_key(self, shard_id: Union[str, int]):
        return self.group_id + '-' + str(shard_id)

    def generate_task_id(self) -> TaskID:
        task_key_int = self._get_next_task_key_int()
        return (
            self._generate_shard_key(task_key_int % self.number_of_shards),
            str(task_key_int)
        )

    @staticmethod
    def report_task_active(task_id: TaskID):
        shard_key, task_key = task_id
        get_redis().hset(shard_key, task_key, time.time())

    @staticmethod
    def report_task_done(task_id: TaskID):
        shard_key, task_key = task_id
        get_redis().hdel(shard_key, task_key)

    def get_remaining_tasks_count(self):
        redis = get_redis()
        counts = gevent.pool.Pool(size=self.number_of_shards).imap_unordered(
            lambda shard_id: redis.hlen(self._generate_shard_key(shard_id)) or 0,
            range(self.number_of_shards)
        )
        return sum(counts)

    def join(self, timeout=0):
        """
        Approximate equivalent of Gevent's or Celery's `.join()`
        where current thread blocks until all tasks are reported done.

        :param Union[int, float] timeout: If set, wait only for this amount of time in seconds.
        :raises: TimeoutError
        """
        timeout_is_set = bool(timeout)
        timeout_time = time.time() + timeout
        period = gevent.getswitchinterval() * 10

        remaining_tasks = self.get_remaining_tasks_count()

        while remaining_tasks:
            if timeout_is_set and timeout_time < time.time():
                raise TimeoutError(f'{self} has {remaining_tasks} remaining tasks after timeout')
            gevent.sleep(period)
            remaining_tasks = self.get_remaining_tasks_count()

    @staticmethod
    def task_context(task_id: Optional[TaskID]):
        return TaskContext(task_id)

    @staticmethod
    def get_task_data(task_id: TaskID):
        return get_redis().hget(*task_id)
