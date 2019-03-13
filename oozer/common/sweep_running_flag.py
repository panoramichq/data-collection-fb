import logging
import random
import ujson as json

from collections import namedtuple, OrderedDict
from typing import Generator, Tuple, List
from contextlib import AbstractContextManager

from common.bugsnag import BugSnagContextData
from common.connect.redis import get_redis
from common.id_tools import parse_id_parts


logger = logging.getLogger(__name__)


class SweepRunningFlag(AbstractContextManager):
    """
    Used to flag (through redis) that a given sweep (by sweep id)
    is running, signalling to celery workers if they still need to run
    or can quit peacefully, early.

    Typically this would be needed when a large sweep is cut short and
    we don't need to run already queued up celery tasks corresponding
    to this sweep
    """

    @staticmethod
    def _generate_key(sweep_id):
        return f'{sweep_id}-running'

    def __init__(self, sweep_id):
        """
        :param SortedJobsQueueInterface sorted_jobs_queue_interface:
        """
        self.sweep_id = sweep_id

    def __enter__(self):
        get_redis().set(self._generate_key(self.sweep_id), 'true')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        get_redis().delete(self._generate_key(self.sweep_id))

    @classmethod
    def is_set(cls, sweep_id):
        return bool(get_redis().get(cls._generate_key(sweep_id)))
