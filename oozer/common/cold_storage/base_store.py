"""
Handles the actual task of moving stuff into the S3 bucket of our choice.

Currently, this is done in-memory, but very possibly, we will need to flush
data to disk first, before we initiate the upload to be more memory stable.


The S3 key must adhere to the following schema:


s3://${s3-bucket-name}/${platform_type}/${account_id_prefix}-${account-id}/${report_type}/${YYYY}/${MM}/${DD}/${report_timestamp}-${report_id}.json  # noqa

Example:

s3://operam-reports/facebook/2d700d-1629501014003404/fb_insights_campaign_daily/2018/02/08/2018-02-08T11:00:00Z-aef8b404-68c7-41f0-a82b-8f7d529d049c.json  # noqa

"""
from typing import Optional, Dict, Any

import boto3
import xxhash
import io
import logging
import uuid
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

from datetime import date, datetime, timezone
from facebook_business.api import FacebookAdsApi
from common.measurement import Measure

import config.aws
import config.build
import common.tztools

from oozer.common.job_scope import JobScope

logger = logging.getLogger(__name__)

# Ensure we are connected to the right endpoint. This is necessary because of
# the faked S3 service, which we contact based on a specific endpoint_url
_s3 = boto3.resource('s3', endpoint_url=config.aws.S3_ENDPOINT)
_bucket = _s3.Bucket(config.aws.S3_BUCKET_NAME)


def _job_scope_to_storage_key(job_scope: JobScope, chunk_marker: Optional[int] = 0) -> str:
    """
    Puts together the S3 object key we need for given report data. This is
    just a helper function

    :param JobScope job_scope: The job scope (dict representation)
    :return string: The full S3 key to use
    """
    assert isinstance(job_scope, JobScope)

    prefix = xxhash.xxh64(job_scope.ad_account_id).hexdigest()[:6]

    # datetime is a subclass of date, so we must check for date first
    if isinstance(job_scope.range_start, date):
        report_datetime = datetime.combine(job_scope.range_start, datetime.min.time())
    elif isinstance(job_scope.range_start, datetime):
        report_datetime = job_scope.range_start
    else:
        # long import line to allow mocking of call to now() in tests.
        report_datetime = common.tztools.now()

    key = f'{job_scope.namespace}/' \
          f'{prefix}-{job_scope.ad_account_id}/' \
          f'{job_scope.report_type}/' \
          f'{report_datetime.strftime("%Y")}/' \
          f'{report_datetime.strftime("%m")}/' \
          f'{report_datetime.strftime("%d")}/' \
          f'{report_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}-' \
          f'{job_scope.job_id}-' \
          f'{str(chunk_marker)+"-" if chunk_marker else ""}' \
          f'{uuid.uuid4()}' \
          f'.json'

    return key


def _job_scope_to_metadata(job_scope: JobScope) -> Dict[str, str]:
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
    if job_scope.ad_account_id == '23845179':
        # We download campaign/adset entity but report on variant
        entity_type = job_scope.report_variant
    else:
        entity_type = job_scope.entity_type or job_scope.report_variant
    return {
        key: value
        for key, value in {
            'build_id': config.build.BUILD_ID,
            'job_id': job_scope.job_id,
            # ISO format in UTC
            'extracted_at': datetime.now(timezone.utc).isoformat(),
            # although all of the below pieces are contained in the
            # job_id, it might be more convenient to have just these
            # in meta straight out.
            'platform': job_scope.namespace,
            'ad_account_id': job_scope.ad_account_id,
            'report_type': job_scope.report_type,
            'entity_type': entity_type,
            # TODO: communicate this with JobScope somewhere
            # so that when we have requests done in multiple versions
            # we communicate the right one.
            'platform_api_version': FacebookAdsApi.API_VERSION,
            'score': None if job_scope.score is None else str(job_scope.score),
        }.items()
        # S3 driver (boto) freaks out about None as values -
        # > value.encode('ascii')
        # E: AttributeError: 'NoneType' object has no attribute 'en
        if value is not None
    }


@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def store(data: Any, job_scope: JobScope, chunk_marker: Optional[int] = 0) -> str:
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
        Key=key, Body=json.dumps(data, ensure_ascii=False).encode(), Metadata=_job_scope_to_metadata(job_scope)
    )

    return key


def load(key):
    ff = io.BytesIO()
    _bucket.download_fileobj(key, ff)
    ff.seek(0)
    return ff


def load_data(key):
    return json.load(load(key))
