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
import json
from datetime import datetime

import config.aws
import config.build
from oozer.common.job_scope import JobScope


class ComplexEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, complex):
            return [obj.real, obj.imag]
        return json.JSONEncoder.default(self, obj)


# Ensure we are connected to the right endpoint. This is necessary because of
# the faked S3 service, which we contact based on a specific endpoint_url
_s3 = boto3.resource('s3', endpoint_url=config.aws.S3_ENDPOINT)
_bucket = _s3.Bucket(config.aws.S3_BUCKET_NAME)


def _job_scope_to_storage_key(job_scope):
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

    key = f'{job_scope.platform}/' \
          f'{prefix}-{job_scope.ad_account_id}/' \
          f'{job_scope.report_type}/' \
          f'{report_run_time.strftime("%Y")}/' \
          f'{report_run_time.strftime("%m")}/' \
          f'{report_run_time.strftime("%d")}/' \
          f'{zulu_time}-{job_scope.job_id}.json'

    # need this to make it work in local fake S3
    return key.replace(':', '_')


def store(data, job_scope):
    """
    Adds the item to the current buffer (by JSON dumping it) and Uploads the
    buffer to S3 under a constructed key

    Note that we take dictionary as input, as opposed to some object. The
    reason behind that is to be more or less oblivious to what it is we are
    actually storing and not have internal knowledge about the data types.

    :param dict data: The raw data to store
    :param JobScope job_scope: The job scope information
    :return string: The key used to store the data
    """
    key = _job_scope_to_storage_key(job_scope)

    _bucket.put_object(
        Key=key,
        Body=json.dumps(data, ensure_ascii=False).encode(),
        Metadata={
            **job_scope.metadata,
            'build_id': config.build.BUILD_ID
        }
    )

    return key
