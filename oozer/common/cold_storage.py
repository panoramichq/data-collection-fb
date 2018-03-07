"""
Handles the actual task of moving stuff into the S3 bucket of our choice.

Currently, this is done in-memory, but very possibly, we will need to flush
data to disk first, before we initiate the upload to be more memory stable.


The S3 key must adhere to the following schema:


s3://${s3-bucket-name}/${platform_type}/${account_id_prefix}-${account-id}/${report_type}/${YYYY}/${MM}/${DD}/${report_timestamp}-${report_id}.json  # noqa

Example:

s3://operam-reports/facebook/2d700d-1629501014003404/fb_insights_campaign_daily/2018/02/08/2018-02-08T11:00:00Z-aef8b404-68c7-41f0-a82b-8f7d529d049c.json  # noqa

"""
import boto3
import gevent
import gevent.pool
import gevent.queue
import hashlib
import logging
import time
# ujson is faster for massive amounts of small data units
# which is actually the pattern we have - yielding small datum per normative
# task or small batches of small datums.
# Biggest problem (for us) with ujson is its handling of very very large numbers
# However, since this code base is dealing with facebook and they committed to
# sending very large numbers as strings, as long as we do NOT convert
# super large (massive spend, or Entity IDs) from stringified form to longs
# we should be fine (and substantially faster on shoving data out us).
# http://artem.krylysov.com/blog/2015/09/29/benchmark-python-json-libraries/
import ujson as json

from collections import namedtuple
from datetime import datetime
from facebookads.api import FacebookAdsApi

import config.aws
import config.build

from oozer.common.job_scope import JobScope


logger = logging.getLogger(__name__)

# Ensure we are connected to the right endpoint. This is necessary because of
# the faked S3 service, which we contact based on a specific endpoint_url
_s3 = boto3.resource('s3', endpoint_url=config.aws.S3_ENDPOINT)
_bucket = _s3.Bucket(config.aws.S3_BUCKET_NAME)


def _job_scope_to_storage_key(job_scope, chunk_marker=0):
    """
    Puts together the S3 object key we need for given report data. This is
    just a helper function

    :param JobScope job_scope: The job scope (dict representation)
    :return string: The full S3 key to use
    """
    assert isinstance(job_scope, JobScope)

    prefix = hashlib.md5(job_scope.ad_account_id.encode()).hexdigest()[:6]
    report_run_time = datetime.utcnow()
    zulu_time = report_run_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    job_id = job_scope.job_id + ('-'+str(chunk_marker) if chunk_marker else '')

    key = f'{job_scope.platform}/' \
          f'{prefix}-{job_scope.ad_account_id}/' \
          f'{job_scope.report_type}/' \
          f'{report_run_time.strftime("%Y")}/' \
          f'{report_run_time.strftime("%m")}/' \
          f'{report_run_time.strftime("%d")}/' \
          f'{zulu_time}-{job_id}.json'

    # need this to make it work in local fake S3
    # return key.replace(':', '_')

    return key


def _job_scope_to_metadata(job_scope):
    """
    Metadata written to S3 (or any other provider) is a little different from
    data we store on the JobScope. Along with *some* data from JobScope
    we push extra identifiers that identify origin code, platform, versioning
    of the payload.

    Some of this stuff (and this method) is use-specific
    (as in used only for ColdStore metadata) and felt like dead weight on JobScope object.

    This is especially specific to S3 because we are limited to 2k of data
    for the entire metadata dict and because of special data clean up needs.
    So, we have to be conservative about what we shove into it, specifically for S3.

    We also compute entity_type value to look like a "normative" report value
    because for all code starting with S3 the difference is irrelevant and all data
    looks like it's "normative."

    :param job_scope:
    :return:
    """
    return {
        key: value
        for key, value in {
            'build_id': config.build.BUILD_ID,
            'job_id': job_scope.job_id,
            # although all of the below pieces are contained in the
            # job_id, it might be more convenient to have just these
            # in meta straight out.
            'platform': job_scope.platform,
            'ad_account_id': job_scope.ad_account_id,
            'report_type': job_scope.report_type,
            'entity_type': job_scope.entity_type or job_scope.report_variant,
            # TODO: communicate this with JobScope somewhere
            # so that when we have requests done in multiple versions
            # we communicate the right one.
            'platform_api_version': FacebookAdsApi.API_VERSION
        }.items()
        # S3 driver (boto) freaks out about None as values -
        # > value.encode('ascii')
        # E: AttributeError: 'NoneType' object has no attribute 'en
        if value is not None
    }


def store(data, job_scope, chunk_marker=0):
    """
    Adds the item to the current buffer (by JSON dumping it) and Uploads the
    buffer to S3 under a constructed key

    Note that we take dictionary as input, as opposed to some object. The
    reason behind that is to be more or less oblivious to what it is we are
    actually storing and not have internal knowledge about the data types.

    :param data: The raw data to store
    :type data: dict or list
    :param JobScope job_scope: The job scope information
    :param chunk_marker: indicator / label hinting that this payload is
        just some portion of this job's total returned data and that this
        particular chunk must be saved under an ID that includes the chunk_marker.
    :return string: The key used to store the data
    """
    key = _job_scope_to_storage_key(job_scope, chunk_marker)

    # per discussion with Mike C, to make Lambda code behind S3 simpler
    # ALL payloads are lists, even those that are single datum.
    if not isinstance(data, (list, tuple, set)):
        data = [data]

    _bucket.put_object(
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode(),
        Metadata=_job_scope_to_metadata(job_scope)
    )

    return key


ColdStoreSave = namedtuple(
    'ColdStoreSave',
    [
        'args',
        'first_attempt_seconds',
        'retry_cnt'
    ]
)


class ColdStoreQueue:
    """
    Turns direct calls to save something into Cold Store into
    an asynchronous backgrounded worker pool that work hard to
    save the thing and retry intelligently.
    """

    max_tries = 3

    def __init__(self, queue_size=None, num_workers=10):
        """
        :param queue_size: If set, makes the queue blocking
        :type queue_size: None or int
        """
        self.queue = gevent.queue.JoinableQueue(queue_size)
        self.pool = gevent.pool.Pool(num_workers)

        # Geventlets do NOT bubble up their errors to parent process
        # They log the error, but not bubble it up
        # This is our trick to make them bubble up
        # All Geventlets can set this single attribute
        self.last_exception = None

        for num in range(num_workers):
            self.pool.spawn(self._print)

    def store(self, data, job_scope, chunk_marker=0):
        if self.last_exception:
            raise self.last_exception

        self.queue.put(
            ColdStoreSave(
                (data, job_scope, chunk_marker),
                time.time(),
                0
            )
        )

    def _print(self):
        while not self.last_exception:
            save = self.queue.get()  # type: ColdStoreSave
            data, job_scope, chunk_marker = save.args

            if data < 10 and data % 2 == 0:
                print(f'< {data} - deferring')
                self.queue.put_nowait(
                    ColdStoreSave(
                        (data + 20, '', ''),
                        save.first_attempt_seconds,
                        save.retry_cnt + 1
                    )
                )
            elif data % 2 == 0:
                self.last_exception = Exception(f'! {data}')
                # print(f'< {data} - start')
                # gevent.sleep(2)
                # print(f'< {data} - end')
            else:
                print(f'< {data} - donzie')

            # try:
            #     store(data, job_scope, chunk_marker)
            # except Exception as ex:
            #     logger.exception(f'This happened {ex}')

            self.queue.task_done()

    def _worker(self):
        while not self.last_exception:
            save = self.queue.get()  # type: ColdStoreSave
            data, job_scope, chunk_marker = save.args

            try:
                store(data, job_scope, chunk_marker)
            except Exception as ex:
                logger.exception(f'While saving to S3 ({save.retry_cnt} try): {ex}')
                self.last_exception = ex
            self.queue.task_done()

        # when exceptions happen, if we don't drain the queue
        # the consuming code may be forever blocked on the queue.put() call
        # here we burn of the rest of the queue in order to
        # get to the error
        while True:
            save = self.queue.get()  # type: ColdStoreSave
            self.queue.task_done()
            logger.exception(f'Discarding cold store bound payload in anticipation of throwing exception')

    def __enter__(self):
        return self.store

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.last_exception:
            self.queue.join()
        self.pool.kill()
        if self.last_exception and not exc_val:
            raise self.last_exception
