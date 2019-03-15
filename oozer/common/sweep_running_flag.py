import logging

from contextlib import AbstractContextManager
from common.connect.redis import get_redis

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
    def _generate_key(sweep_id: str) -> str:
        return f'{sweep_id}-running'

    def __init__(self, sweep_id: str):
        self.sweep_id = sweep_id

    def __enter__(self) -> 'SweepRunningFlag':
        get_redis().set(self._generate_key(self.sweep_id), 'true')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        get_redis().delete(self._generate_key(self.sweep_id))

    @classmethod
    def is_set(cls, sweep_id: str) -> bool:
        return bool(get_redis().get(cls._generate_key(sweep_id)))
