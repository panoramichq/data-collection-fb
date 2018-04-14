import bugsnag
import ujson as json
import logging
import random

from collections import namedtuple, OrderedDict
from typing import Generator, Tuple, List

from common.connect.redis import get_redis
from common.id_tools import parse_id_parts


logger = logging.getLogger(__name__)


class NotSet:
    pass


class _JobsWriter:

    def __init__(self, sorted_jobs_queue_interface, batch_size=30):
        """
        Closure that exposes a callable that gets repeatedly called with item to add to one and same
        SortedSet Redis key. The focus is on keeping track of some (batch_size) last additions
        in memory and flushing out bundles of inserts to Redis.

        An extra bonus in this particular case is help with repeat keys.
        These collapse onto themselves in memory first, so writing out
        20 distinct ID-score pairs as 1000 repeated ID-score pairs
        will result in only one 20-strong batch written, where ID-score
        pairs written will be those that are seen last under distinct ID.

        We are saving IO hits to Redis on duplicate inserts.
        We also know / expect that duplication comes in series (per our generator code)
        where same Job id (with potentially gradually incrementing score) is communicated
        as a stream, before a switch to a stream of other ID.

        :param SortedJobsQueueInterface sorted_jobs_queue_interface:
        """
        self.batch = {}
        self.batch_size = batch_size
        self.cache = OrderedDict()
        self.processed_job_scope_data_ad_account_ids = set()
        self.cache_max_size = 700
        self.cnt = 0
        self.redis_client = get_redis()
        self.sweep_id = sorted_jobs_queue_interface.sweep_id
        self.sorted_jobs_queue_interface = sorted_jobs_queue_interface

    def flush(self):
        # zadd takes a list of key, score, key2, score2, ... arguments
        # we need to convert dict into list of key, value, key2, value2, ...
        args = [
            item
            for pair in self.batch.items()
            for item in pair
        ]

        self.redis_client.zadd(
            # since we do a batch, there is no particular reason to
            # provide a value for deterministic key shart. Random it is.
            self.sorted_jobs_queue_interface.get_queue_key(),
            *args
        )
        self.cnt += len(self.batch)
        self.batch.clear()

    def write_job_scope_data(self, job_scope_data, job_id_parts):
        # scope_data are chunks of data we elsewhere refer to as "petals" in Data Flower
        # these are units of context data that were not encoded into the job ID,
        # but that are still needed for successful execution of the job.
        # what you have there are things like ad account's time zone, possibly tokens,
        # possibly
        # However, saving potentially same exact thing for all millions of jobs queued up would be nuts
        # So, instead, we'll save the data only for every new ad account ID we see.
        if job_id_parts.ad_account_id not in self.processed_job_scope_data_ad_account_ids:
            self.processed_job_scope_data_ad_account_ids.add(job_id_parts.ad_account_id)
            self.redis_client.set(
                self.sorted_jobs_queue_interface.get_payload_key(job_id_parts.ad_account_id),
                json.dumps(job_scope_data)
            )

    def add_to_queue(self, job_id, score, **job_scope_data):
        job_id_parts = parse_id_parts(job_id)

        if job_scope_data:
            self.write_job_scope_data(job_scope_data, job_id_parts)

        if job_id_parts.entity_id:
            # if entity ID is present, there is no way
            # this could be a reused job
            # (as only per-Parent jobs can be reused by children)
            # Thus, we just add to the batch and move on
            self.batch[job_id] = score
        else: # this is some per-Parent job
            # There is a chance it's in the larger cache with same exact score
            if self.cache.get(job_id) == score:
                # if it's there with same score, it was already flushed out
                # as such. No point even adding it to batch.
                pass
            else:
                # the job is either there with different score, or not there,
                # but either way we need to write it out with a new score
                self.batch[job_id] = score
                self.cache[job_id] = score
                while len(self.cache) > self.cache_max_size:
                    self.cache.popitem()  # oldest

        if len(self.batch) == self.batch_size:
            self.flush()

    def __enter__(self):
        return self.add_to_queue

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.batch: # some sub-batch_size leftovers
            self.flush()
        logger.info(f"#{self.sweep_id}: Redis SortedSet Batcher wrote a total of {self.cnt} *unique* tasks")


FrontRowParticipant = namedtuple(
    'FrontRowParticipant',
    [
        'score',
        'job_id',
        'gen'
    ]
)


class _JobsReader:

    def __init__(self, sorted_jobs_queue_interface, batch_size=200):
        """
        :param SortedJobsQueueInterface sorted_jobs_queue_interface:
        """
        self.batch_size = batch_size
        self.cnt = 0
        self.ad_account_id_job_scope_data_map = OrderedDict()
        self.sorted_jobs_queue_interface = sorted_jobs_queue_interface

    @staticmethod
    def _iter_tasks_per_key(key, redis, batch_size):
        start = 0
        step = batch_size

        job_id_score_pairs = redis.zrevrange(key, start, start+step, withscores=True)
        while job_id_score_pairs:
            for job_id_score_pair in job_id_score_pairs:

                try:
                    job_id, score = job_id_score_pair
                except ValueError as ex:
                    # "Too many values to unpack" errors in production.
                    # Unable to replicate these in local env and don't have enough data from
                    # exception to figure out what the deal is.
                    # Instrumenting this code per:
                    # https://docs.bugsnag.com/platforms/python/other/#sending-diagnostic-data
                    # In order to see more context data next time this happens.
                    if 'too many values to unpack' or 'not enough values to unpack' in str(ex):
                        try:
                            job_id_score_pair_len = len(job_id_score_pair)
                        except:
                            job_id_score_pair_len = 'cannot be determined'

                        import pickle
                        import base64
                        bugsnag.notify(
                            ex,
                            meta_data={
                                # this will become a tab in BugSnag
                                'call_context_info': {
                                    'job_id_score_pair_type': type(job_id_score_pair),
                                    'job_id_score_pair_len': job_id_score_pair_len,
                                    'job_id_score_pair_data': base64.b64encode(pickle.dumps(job_id_score_pair)).decode('ascii')
                                }
                            }
                        )
                        # and exit cleanly
                        return
                    else:
                        raise ex

                yield job_id.decode('utf8'), score

            start += step
            job_id_score_pairs = redis.zrevrange(key, start, start+step)

    def read_job_scope_data(self, job_id, max_cache_size=4000):
        """
        :param job_id:
        :param max_cache_size:
        :rtype: dict
        """
        # This is the Read part of Data Flower code in _JobsWriter's add_to_queue
        # Here we pick up that auxiliary data that Writer created for this job
        # Note that for now, Writer creates a per-ad_account_id payloads only
        # So, we don't have to read data payloads for every job. Only first
        # time we see a given ad account ID do we have to go and fetch the data.
        # Hence, here we don't read the data by JobID but by ad_account_id.
        job_id_parts = parse_id_parts(job_id)
        job_data = self.ad_account_id_job_scope_data_map.get(job_id_parts.ad_account_id, NotSet)
        if job_data is NotSet:
            job_data = json.loads(
                get_redis().get(
                    self.sorted_jobs_queue_interface.get_payload_key(job_id_parts.ad_account_id)
                ) or '{}'
            )
            self.ad_account_id_job_scope_data_map[job_id_parts.ad_account_id] = job_data
            while len(self.ad_account_id_job_scope_data_map) > max_cache_size: #
                self.ad_account_id_job_scope_data_map.popitem()  # oldest

        return job_data

    def iter_jobs(self):
        """
        Joins streams (paging generators) of data from multiple SortedSet Redis keys
        and converts it into a single stream (generator) of JobID, job Payload pairs
        still sorted in proper order by score inspite of being multiplexed from
        multiple steams of scored data.

        If we have 10 shards (10 separate SortedSet keys in Redis) with millions of
        records shared between them, this code will lazily consume
        *one* at the time (sorted in reverse score order, fetched in pages of 200
        per SortedSet key) from each of 10 streams, and will yield highest
        scored jobID between the 10 we see on the front row, asking the generator
        from which it took to refill the front row. Repeat.

        With 10 shards and paging of 200 elements per stream we will hold up to 2000
        elements in memory, while effectively streaming uniformly sorted stream
        of millions of jobs.
        """

        keys = self.sorted_jobs_queue_interface.get_queue_keys_range()
        redis = get_redis()

        ### prep

        job_id_score_generators = [
            self._iter_tasks_per_key(key, redis, self.batch_size)
            for key in keys
        ]
        # list of tuples like (score, job_id, generator_of_next_score-job-per-key)
        front_row = []  # type: List[Tuple[int, str, Generator]]
        for gen in job_id_score_generators:
            try:
                job_id, score = gen.__next__()
                front_row.append(
                    FrontRowParticipant(score, job_id, gen)
                )
            except StopIteration:
                # nothing in this key
                pass

        ### use

        while front_row:
            front_row.sort(key=lambda o: o.score, reverse=True)
            score, job_id, gen = front_row.pop(0)

            ##### \/ this is the actual signature of the iterator we return \/ #####
            yield job_id, self.read_job_scope_data(job_id), score
            ##### /\ this is the actual signature of the iterator we return /\ #####

            self.cnt += 1

            try:
                job_id, score = gen.__next__()
                front_row.append(
                    FrontRowParticipant(score, job_id, gen)
                )
            except StopIteration:
                # nothing in this key anymore
                # not adding it at all to front_row
                pass

    def __enter__(self):
        return self.iter_jobs()

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(f"#{self.sorted_jobs_queue_interface.sweep_id}: Redis SortedSet Task Reader read a total of {self.cnt} tasks")


class SortedJobsQueue:
    """
    A pool of SortedSet redis keys and auxiliary storage keys
    abstracted to our code as single queue for Jobs (and metadata)
    insertion, off-process-sorting (by score) and streamed retrieval.

    Used for both, inserting jobs in the queue (in Sweep Builder)
    and for reading jobs from the queue (in Sweep Looper).
    """

    def __init__(self, sweep_id):
        """
        Closure that exposes a callable that gets repeatedly called with item to add to one and same
        SortedSet Redis key. The focus is on keeping track of some (batch_size) last additions
        in memory and flushing out bundles of inserts to Redis.

        An extra bonus in this particular case is help with repeat keys.
        These collapse onto themselves in memory first, so writing out
        20 distinct ID-score pairs as 1000 repeated ID-score pairs
        will result in only one 20-strong batch written, where ID-score
        pairs written will be those that are seen last under distinct ID.

        We are saving IO hits to Redis on duplicate inserts.
        We also know / expect that duplication comes in series (per our generator code)
        where same Job id (with potentially gradually incrementing score) is communicated
        as a stream, before a switch to a stream of other ID.

        :param sweep_id:
        :param batch_size:
        """
        self.sweep_id = sweep_id
        self._queue_key_base = f'{sweep_id}-sorted-jobs-queue-'
        self._payload_key_base = f'{sweep_id}-sorted-jobs-data-'

    def get_payload_key(self, ad_account_id):
        """
        For the time being we are optimizing the store of job data
        to be per parent Ad Account (so that we write it to Redis
        only once per AdAccountID as opposed to for every job.
        :param ad_account_id:
        :return:
        """
        return self._payload_key_base + ad_account_id

    def get_queue_key(self, value=None, shard_id=None):
        if shard_id is not None:
            return self._queue_key_base + str(shard_id)
        if value:
            # since we take just the last decimal from the hash int
            # effectively we have 10 shards
            return self._queue_key_base + str(hash(value))[-1]
        # same here - 10 possible shards
        return self._queue_key_base + str(random.randint(0,9))

    def get_queue_keys_range(self):
        # still same 10 shards
        return [
            self.get_queue_key(shard_id=i)
            for i in range(0,10)  # last arg is exclusive not inclusive
        ]

    def get_queue_length(self):
        cnt = 0
        redis = get_redis()
        for key in self.get_queue_keys_range():
            cnt += redis.zcount(key, '-inf', '+inf') or 0
        return cnt

    def JobsWriter(self):
        """

        Example::

            with SortedJobsQueue(sweep_id).JobsWriter() as add_to_queue:
                for job_id, score in scored_jobs:
                    # writes tasks to distributed sorting queues
                    add_to_queue(job_id, score, **job_scope_extra_data)

        :return:
        """
        return _JobsWriter(self)

    def JobsReader(self):
        """

        Example:

            with SortedJobsQueue(sweep_id).JobsReader() as jobs_iter:
                # tasks_iter yields job_ids sorted by insertion score
                for job_id, job_data, score in jobs_iter:
                    do_something(job_id)

        :return:
        """
        return _JobsReader(self)
