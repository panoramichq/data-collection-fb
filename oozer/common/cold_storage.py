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
import hashlib
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

from datetime import datetime

import config.aws
import config.build

from oozer.common.job_scope import JobScope


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

    _bucket.put_object(
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode(),
        Metadata={
            **job_scope.metadata,
            'build_id': config.build.BUILD_ID
        }
    )

    return key
