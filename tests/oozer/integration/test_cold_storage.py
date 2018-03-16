# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import boto3
import config.aws
import config.build
import hashlib
import json
import uuid

from datetime import datetime, timedelta
from io import BytesIO

from common import tztools
from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from facebookads.adobjects.campaign import Campaign
from oozer.common import cold_storage
from oozer.common.job_scope import JobScope
from tests.base.random import gen_string_id


class TestUploadToS3(TestCase):

    _s3 = boto3.resource('s3', endpoint_url=config.aws.S3_ENDPOINT)
    _bucket = _s3.Bucket(config.aws.S3_BUCKET_NAME)

    def setUp(self):
        super().setUp()

        self.campaign_id = gen_string_id()
        self.ad_account_id = gen_string_id()

    def _fake_data_factory(self, fbid=None, **data):
        """
        Fake campaign factory for testing purposes

        :return Campaign: Facebook SDK campaign object
        """
        test_campaign = Campaign(fbid or self.campaign_id)
        test_campaign[Campaign.Field.account_id] = self.ad_account_id
        test_campaign.update(data)
        return test_campaign.export_all_data()

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
            test_campaign,
            job_scope
        )

        _, fileobj_under_test = self._get_s3_object(storage_key)

        # Note that the value is an array of one or more elements.
        # it's always an array
        assert json.load(fileobj_under_test) == [{
            "id": self.campaign_id,
            "account_id": self.ad_account_id
        }]

    def test_metadata_stored(self):
        """
        Check we have stored the expected metadata
        """
        test_data = self._fake_data_factory()
        ctx = JobScope(
            ad_account_id=test_data[Campaign.Field.account_id],
            report_type=ReportType.entities,
        )

        run_start = datetime.utcnow()

        storage_key = cold_storage.store(test_data, ctx)
        s3_obj, _ = self._get_s3_object(storage_key)

        run_end = datetime.utcnow()

        # .store() generates its own timestamp, so checking
        # for exact time is not possible. hence we did the _start, _end range.
        # it must be an ISO string in UTC
        extracted_at = s3_obj.metadata.pop('extracted_at')
        # this mast parse without errors.
        # error here means value is not present or is wrong format.
        dt = datetime.strptime(
            extracted_at,
            # '2018-03-10T02:31:44.874854+00:00'
            '%Y-%m-%dT%H:%M:%S.%f+00:00'
        )
        # some number of microseconds must have passed,
        # so testing in the range.
        assert dt >= run_start
        assert dt <= run_end

        assert s3_obj.metadata == {
            'build_id': config.build.BUILD_ID,
            'job_id': ctx.job_id,
            'platform': 'fb',
            'ad_account_id': ctx.ad_account_id,
            'report_type': ReportType.entities,
            'platform_api_version': 'v2.11'
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
        expected_key = f'fb/{account_prefix}-{params["ad_account_id"]}' \
                       f'/{params["report_type"]}' \
                       f'/{params["report_time"].strftime("%Y")}' \
                       f'/{params["report_time"].strftime("%m")}' \
                       f'/{params["report_time"].strftime("%d")}/{zulu_time}-' \
                       f'{scope.job_id}.json'

        assert storage_key == expected_key
