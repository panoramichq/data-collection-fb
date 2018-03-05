# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import boto3
import config.aws
import config.build
import hashlib
import json
import uuid
import pytz

from datetime import datetime
from io import BytesIO

from common import tztools
from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from facebookads.adobjects.campaign import Campaign
from oozer.common import cold_storage
from oozer.common.job_scope import JobScope
from tests.base.random import get_string_id


class TestUploadToS3(TestCase):

    _s3 = boto3.resource('s3', endpoint_url=config.aws.S3_ENDPOINT)
    _bucket = _s3.Bucket(config.aws.S3_BUCKET_NAME)

    def setUp(self):
        super().setUp()

        self.campaign_id = get_string_id()
        self.ad_account_id = get_string_id()

    def _fake_data_factory(self, fbid=None, **data):
        """
        Fake campaign factory for testing purposes

        :return Campaign: Facebook SDK campaign object
        """
        test_campaign = Campaign(self.campaign_id)
        test_campaign[Campaign.Field.account_id] = self.ad_account_id
        test_campaign.update(data)
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
        known_object_contents = f'{{"id": "{self.campaign_id}", "account_id": "{self.ad_account_id}"}}'.encode('utf8')

        test_campaign = self._fake_data_factory(
            self.campaign_id,
            **{
                Campaign.Field.account_id: self.ad_account_id
            }
        )

        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            entity_id=self.campaign_id,
            entity_type=Entity.Campaign,
            report_type=ReportType.entities,
            range_start='2017-12-31',
        )

        storage_key = cold_storage.store(
            test_campaign.export_all_data(),
            job_scope
        )

        _, fileobj_under_test = self._get_s3_object(storage_key)

        # Assert the contents
        assert json.load(fileobj_under_test) == {"id": self.campaign_id, "account_id": self.ad_account_id}

    def test_metadata_stored(self):
        """
        Check we have stored the expected metadata
        """
        test_data = self._fake_data_factory()
        ctx = JobScope(
            ad_account_id=test_data[Campaign.Field.account_id],
            report_type='fb_entities_adaccount_campaigns',
            report_time=datetime.now(pytz.utc),
            report_id=uuid.uuid4().hex,
            # Construct date the same way as the sweep builder does it
            range_start=datetime.strptime('2017-01-01', '%Y-%m-%d'),
            range_end=datetime.strptime('2017-01-02', '%Y-%m-%d')
        )

        storage_key = cold_storage.store(dict(test_data), ctx)
        s3_obj, _ = self._get_s3_object(storage_key)

        assert s3_obj.metadata == {
            'build_id': config.build.BUILD_ID,
            'ad_account_id': test_data[Campaign.Field.account_id],
            'entity_id': 'None',
            'entity_type': 'None',
            'platform': 'facebook',
            'range_start': '2017-01-01',
            'range_end': '2017-01-02',
            'report_type': 'fb_entities_adaccount_campaigns',
            'report_variant': 'None',
        }

    def test_key_s3_construction(self):
        """
        Check that the key is constructed as we expect
        """
        test_data = self._fake_data_factory()

        params = {
            'ad_account_id': test_data[Campaign.Field.account_id],
            'report_type': 'fb_entities_adaccount_campaigns',
            'report_time': tztools.now_in_tz('UTC'),
            'report_id': uuid.uuid4().hex,
            'request_metadata': {}
        }

        scope = JobScope(**params)

        storage_key = cold_storage.store(dict(test_data), scope)

        account_prefix = hashlib.md5(params['ad_account_id'].encode()) \
            .hexdigest()[:6]

        zulu_time = params["report_time"].strftime("%Y-%m-%dT%H:%M:%SZ")
        expected_key = f'facebook/{account_prefix}-{params["ad_account_id"]}' \
                       f'/{params["report_type"]}' \
                       f'/{params["report_time"].strftime("%Y")}' \
                       f'/{params["report_time"].strftime("%m")}' \
                       f'/{params["report_time"].strftime("%d")}/{zulu_time}-' \
                       f'{scope.job_id}.json'

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

        scope = JobScope(**params)

        account_prefix = hashlib.md5(params['ad_account_id'].encode()) \
            .hexdigest()[:6]
        zulu_time = tztools.dt_to_other_timezone(params['report_time'], 'UTC')
        expected_key = f'facebook/{account_prefix}-1' \
                       f'/rt/' \
                       f'{zulu_time.strftime("%Y")}/' \
                       f'{zulu_time.strftime("%m")}/' \
                       f'{zulu_time.strftime("%d")}/' \
                       f'{zulu_time.strftime("%Y-%m-%dT%H:%M:%SZ")}-' \
                       f'{scope.job_id}.json'

        assert cold_storage._job_scope_to_storage_key(scope) == expected_key
