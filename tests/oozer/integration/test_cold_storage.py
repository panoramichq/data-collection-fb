# mustfrom t be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
from datetime import datetime
import uuid
import boto3
import config.aws
import config.build
import hashlib
import pytest
from io import BytesIO
import pytz
import dateutil.parser

from facebookads.adobjects.campaign import Campaign
from oozer.common import cold_storage, job_scope
from common import tztools


class TestUploadToS3(TestCase):

    _s3 = boto3.resource('s3', endpoint_url=config.aws.S3_ENDPOINT)
    _bucket = _s3.Bucket(config.aws.S3_BUCKET_NAME)

    def _fake_data_factory(self):
        """
        Fake campaign factory for testing purposes

        :return Campaign: Facebook SDK campaign object
        """
        test_campaign = Campaign('123123123')
        test_campaign[Campaign.Field.account_id] = '98989898'
        return test_campaign

    def _get_s3_object(self, key):
        """
        A helper to download to obtain the S3 object

        :param string key: The key to lookup
        :return:
        """
        fileobj = BytesIO()
        s3_object = self._bucket.Object(key=key)
        s3_object.download_fileobj(fileobj)
        fileobj.seek(0)

        return s3_object, fileobj

    def test_uploads_data_successfully(self):
        """
        Test the basic upload
        """
        known_object_contents = b'{"id": "123123123", "account_id": "98989898"}'

        test_data = self._fake_data_factory()

        ctx = job_scope.JobScope(
            ad_account_id=test_data[Campaign.Field.account_id],
            report_type='fb_entities_adaccount_campaigns',
            report_time=datetime.now(pytz.utc),
            report_id=uuid.uuid4().hex,
            request_metadata={}
        )

        storage_key = cold_storage.store(dict(test_data), ctx)
        _, fileobj_under_test = self._get_s3_object(storage_key)

        # Assert the contents
        assert fileobj_under_test.read() == known_object_contents

    def test_metadata_stored(self):
        """
        Check we have stored the expected metadata
        """
        test_data = self._fake_data_factory()
        ctx = job_scope.JobScope(
            ad_account_id=test_data[Campaign.Field.account_id],
            report_type='fb_entities_adaccount_campaigns',
            report_time=datetime.now(pytz.utc),
            report_id=uuid.uuid4().hex,
            metadata={
                'some': 'metadata'
            }
        )

        storage_key = cold_storage.store(dict(test_data), ctx)
        s3_obj, _ = self._get_s3_object(storage_key)

        assert s3_obj.metadata == {
            'build_id': config.build.BUILD_ID,
            'some': 'metadata'
        }

    def test_key_s3_construction(self):
        """
        Check that the key is constructed as we expect
        """
        test_data = self._fake_data_factory()

        params = {
            'ad_account_id': test_data[Campaign.Field.account_id],
            'report_type': 'fb_entities_adaccount_campaigns',
            'report_time': datetime.now(pytz.utc),
            'report_id': uuid.uuid4().hex,
            'request_metadata': {}
        }

        ctx = job_scope.JobScope(**params)
        storage_key = cold_storage.store(dict(test_data), ctx)

        account_prefix = hashlib.md5(params['ad_account_id'].encode()) \
            .hexdigest()[:6]

        zulu_time = params["report_time"].strftime("%Y-%m-%dT%H:%M:%SZ")
        expected_key = f'facebook/{account_prefix}-{params["ad_account_id"]}' \
                       f'/{params["report_type"]}' \
                       f'/{params["report_time"].strftime("%Y")}' \
                       f'/{params["report_time"].strftime("%m")}' \
                       f'/{params["report_time"].strftime("%d")}/{zulu_time}-' \
                       f'{params["report_id"]}.json'

        assert storage_key == expected_key

    def test_tz_handling(self):
        """
        Checks that timezone gets converted correctly and that datetime without
        tz is not accepted
        """
        params = {
            'ad_account_id': '1',
            'report_type': 'rt',
            'report_time': tztools.now_in_tz('America/Los_Angeles'),
            'report_id': '1',
            'request_metadata': {}
        }

        # Create a fixed time in some weird TZ
        params['report_time'] = dateutil.parser.parse(
            "2010-01-01T00:00:00-08:00"
        )
        ctx = job_scope.JobScope(**params)

        account_prefix = hashlib.md5(params['ad_account_id'].encode()) \
            .hexdigest()[:6]
        zulu_time = '2010-01-01T08:00:00Z'
        expected_key = f'facebook/{account_prefix}-1' \
                       f'/rt/2010/01/01/{zulu_time}-1.json'

        assert cold_storage._context_as_storage_key(ctx) == expected_key
