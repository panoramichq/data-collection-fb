import boto3
import hashlib
import io

from config import aws, build

# Ensure we are connected to the right endpoint. This is necessary because of
# the faked S3 service, which we contact based on a specific endpoint_url
if aws.S3_ENDPOINT is not None:
    print("configuring the endpoint")
    _s3 = boto3.resource('s3', endpoint_url=aws.S3_ENDPOINT)
else:
    _s3 = boto3.resource('s3')


_bucket = _s3.Bucket(aws.S3_BUCKET_NAME)


class ColdStorageUploader:
    """
    Handles the actual task of moving stuff into the S3 bucket of our choice.

    Currently, this is done in-memory, but very possibly, we will need to flush
    data to disk first, before we initiate the upload to be more memory stable.


    The S3 key must adhere to the following schema:


    s3://${s3-bucket-name}/${platform_type}/${account_id_prefix}-${account-id}/${report_type}/${YYYY}/${MM}/${DD}/${report_timestamp}-${report_id}.json  # noqa

    Example:

    s3://operam-reports/facebook/2d700d-1629501014003404/fb_insights_campaign_daily/2018/02/08/2018-02-08T11:00:00Z-aef8b404-68c7-41f0-a82b-8f7d529d049c.json  # noqa

    """

    PLATFORM = 'facebook'

    def __init__(self, ad_account_id, report_type, report_time, report_id, request_metadata):
        """
        Initialize the storage with arguments required to store it

        :param string ad_account_id:
        :param string report_type:
        :param datetime.datetime report_time:
        :param string report_id:
        :param dict request_metadata:
        """
        self._ad_account_id = ad_account_id
        self._report_type = report_type
        self._report_time = report_time
        self._report_id = report_id

        # Add build id to metadata
        self._metadata = {
            **request_metadata,
            'build_id': build.BUILD_ID
        }

        # We're starting with opening brackets for a JSON list. Not nice really,
        # but does the trick
        self._buffer = io.BytesIO()

    def _get_storage_key(self):
        """
        Puts together the S3 object key we need for given report data

        :return string: The full S3 key to use
        """
        prefix = hashlib.md5(self._ad_account_id.encode()).hexdigest()[:6]

        return f'facebook/{prefix}-{self._ad_account_id}/{self._report_type}/' \
               f'{self._report_time.year}/{self._report_time.month}/' \
               f'{self._report_time.day}/{self._report_time.isoformat()}-' \
               f'{self._report_id}.json'

    def write_items(self, data):
        """
        Adds the data to the current buffer. This method takes a list and just
        dumbly dumps it to JSON and the buffer
        """
        # TODO: This is obviously problematic, as we create 2x the memory
        # requirement. The reason why this is here is because it is as of now
        # not obvious, what the format of the data is / will be, and how / when
        # they will be received from the collection task.

        # Once this becomes settled, the method will be updated accordingly
        pass

    def store(self):
        """
        Uploads the current buffer to S3 under a constructed key

        Close out the buffer with closing list bracket, seek to the beginning
        and throw it at S3 upload.
        """

        self._buffer.write(b']')
        self._buffer.seek(0)

        _bucket.put_object(
            Key=self._get_storage_key(),
            Body=self._buffer,
            Metadata=self._metadata
        )
