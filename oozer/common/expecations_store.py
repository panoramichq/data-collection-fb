import logging

from queue import deque

from common.connect.redis import get_redis
from common.id_tools import parse_id


logger = logging.getLogger(__name__)


# precompiled templates for key generation
_sweep_aa_index_key_template = '{sweep_id}-sweep-aa-index-set'.format
_aa_job_index_key_template = '{sweep_id}-{ad_account_id}-expectation-aa-index-set'.format
_job_expectation_key_template = '{sweep_id}-{job_id}-expectations'.format


class JobExpectationsWriter:

    def __init__(self, sweep_id, cache_max_size=2000):

        self.sweep_id = sweep_id

        self._aa_cache = set()
        self._aa_cache_order = deque()
        self._aa_cache_remaining = cache_max_size

        self._job_id_cache = set()
        self._job_id_cache_order = deque()
        self._job_id_cache_remaining = cache_max_size

        self._redis_client = get_redis()

    def add(self, job_id, ad_account_id, entity_id):

        key_template_data = dict(
            sweep_id=self.sweep_id,
            job_id=job_id,
            ad_account_id=ad_account_id,
            entity_id=entity_id
        )

        if ad_account_id not in self._aa_cache:
            self._aa_cache.add(ad_account_id)
            # we are adding to cache in FIFO mode
            self._aa_cache_order.append(ad_account_id)  # on right side

            if self._aa_cache_remaining == 0:
                # we are popping off cache in FIFO order
                item = self._aa_cache_order.popleft()
                self._aa_cache.remove(item)
            else:
                self._aa_cache_remaining -= 1

            self._redis_client.sadd(
                _sweep_aa_index_key_template(**key_template_data),
                ad_account_id
            )

        # job-aa index record
        if job_id not in self._job_id_cache:
            self._job_id_cache.add(job_id)
            # we are adding to cache in FIFO mode
            self._job_id_cache_order.append(job_id)  # on right side

            if self._job_id_cache_remaining == 0:
                # we are popping off cache in FIFO order
                item = self._job_id_cache_order.popleft()
                self._job_id_cache.remove(item)
            else:
                self._job_id_cache_remaining -= 1

            self._redis_client.sadd(
                _aa_job_index_key_template(**key_template_data),
                job_id
            )

        # At this point we are never scheduling per-entity jobs
        # Thus, we don't need to record the expectations at per-entity level.
        # self._redis_client.hset(
        #     _job_expectation_key_template(job_id_effective=job_id),
        #     entity_id,
        #     self.sweep_id
        # )

    def __enter__(self):
        return self.add

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def iter_expectations(sweep_id):

    redis = get_redis()
    sweep_level_key = _sweep_aa_index_key_template(
        sweep_id=sweep_id
    )
    for ad_account_id in redis.sscan_iter(sweep_level_key):
        ad_account_id = ad_account_id.decode('utf8')
        aa_level_key = _aa_job_index_key_template(
            sweep_id=sweep_id,
            ad_account_id=ad_account_id
        )
        for job_id in redis.sscan_iter(aa_level_key):
            yield job_id.decode('utf8')
