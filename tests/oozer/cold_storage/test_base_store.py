# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

import boto3
import config.aws
import config.build
import xxhash
import json
import uuid

from datetime import date, datetime
from io import BytesIO

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from facebook_business.adobjects.campaign import Campaign
from oozer.common import cold_storage
from oozer.common.job_scope import JobScope
from oozer.common.cold_storage.base_store import _job_scope_to_metadata
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
        test_campaign = self._fake_data_factory(self.campaign_id, **{Campaign.Field.account_id: self.ad_account_id})

        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            entity_id=self.campaign_id,
            entity_type=Entity.Campaign,
            report_type=ReportType.entity,
            range_start='2017-12-31',
        )

        storage_key = cold_storage.store(test_campaign, job_scope)

        _, fileobj_under_test = self._get_s3_object(storage_key)

        # Note that the value is an array of one or more elements.
        # it's always an array
        assert json.load(fileobj_under_test) == [{"id": self.campaign_id, "account_id": self.ad_account_id}]

    def test_metadata_stored(self):
        """
        Check we have stored the expected metadata
        """
        test_data = self._fake_data_factory()
        ctx = JobScope(ad_account_id=test_data[Campaign.Field.account_id], report_type=ReportType.entity)

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
            '%Y-%m-%dT%H:%M:%S.%f+00:00',
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
            'report_type': ReportType.entity,
            'platform_api_version': 'v4.0',
        }


class TestingS3KeyGeneration(TestCase):
    def test_key_s3_date_less(self):
        """
        Check that the key is constructed as we expect
        """
        import common.tztools

        job_scope = JobScope(
            ad_account_id=gen_string_id(), report_type=ReportType.entity, report_variant=Entity.Campaign
        )

        now_dt = datetime(2000, 1, 2, 3, 4, 5)
        with mock.patch.object(common.tztools, 'now', return_value=now_dt) as now_mocked, mock.patch.object(
            uuid, 'uuid4', return_value='UUID-HERE'
        ):

            storage_key = cold_storage.store({'data': 'yeah!'}, job_scope)

        assert now_mocked.called

        prefix = xxhash.xxh64(job_scope.ad_account_id).hexdigest()[:6]

        expected_key = (
            f'fb/'
            + f'{prefix}-{job_scope.ad_account_id}/'
            + f'{job_scope.report_type}/'
            + f'{now_dt.strftime("%Y")}/'
            + f'{now_dt.strftime("%m")}/'
            + f'{now_dt.strftime("%d")}/'
            + f'{now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}-'
            + f'{job_scope.job_id}-'
            + f'UUID-HERE'
            + f'.json'
        )

        assert storage_key == expected_key

    def test_key_s3_incomprehensible_range_start(self):
        """
        Check that the key is constructed as we expect
        """
        import common.tztools

        job_scope = JobScope(
            ad_account_id=gen_string_id(),
            report_type=ReportType.day_platform,
            report_variant=Entity.Campaign,
            range_start='blah-blah',
        )

        # even though range_start is provided ^ above, it's not date-like and we
        # should be ok with that and just fall back to datetime.utcnow()
        now_dt = datetime(2000, 1, 2, 3, 4, 5)
        with mock.patch.object(common.tztools, 'now', return_value=now_dt) as now_mocked, mock.patch.object(
            uuid, 'uuid4', return_value='UUID-HERE'
        ):

            storage_key = cold_storage.store({'data': 'yeah!'}, job_scope)

        assert now_mocked.called

        prefix = xxhash.xxh64(job_scope.ad_account_id).hexdigest()[:6]

        expected_key = (
            f'fb/'
            + f'{prefix}-{job_scope.ad_account_id}/'
            + f'{job_scope.report_type}/'
            + f'{now_dt.strftime("%Y")}/'
            + f'{now_dt.strftime("%m")}/'
            + f'{now_dt.strftime("%d")}/'
            + f'{now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}-'
            + f'{job_scope.job_id}-'
            + f'UUID-HERE'
            + f'.json'
        )

        assert storage_key == expected_key

    def test_key_s3_date_snapped(self):
        """
        Check that the key is constructed as we expect
        """

        job_scope = JobScope(
            ad_account_id=gen_string_id(),
            report_type=ReportType.day_platform,
            report_variant=Entity.Ad,
            range_start=date(2000, 1, 2),
        )

        dt_should_be = datetime(2000, 1, 2, 0, 0, 0)
        with mock.patch.object(uuid, 'uuid4', return_value='UUID-HERE'):
            storage_key = cold_storage.store({'data': 'yeah!'}, job_scope)

        prefix = xxhash.xxh64(job_scope.ad_account_id).hexdigest()[:6]
        expected_key = (
            f'fb/'
            + f'{prefix}-{job_scope.ad_account_id}/'
            + f'{job_scope.report_type}/'
            + f'{dt_should_be.strftime("%Y")}/'
            + f'{dt_should_be.strftime("%m")}/'
            + f'{dt_should_be.strftime("%d")}/'
            + f'{dt_should_be.strftime("%Y-%m-%dT%H:%M:%SZ")}-'
            + f'{job_scope.job_id}-'
            + f'UUID-HERE'
            + f'.json'
        )

        assert storage_key == expected_key

    def test_key_s3_date_snapped_with_chunk_id(self):
        """
        Check that the key is constructed as we expect
        """

        job_scope = JobScope(
            ad_account_id=gen_string_id(),
            report_type=ReportType.day_platform,
            report_variant=Entity.Ad,
            range_start=date(2000, 1, 2),
        )

        chunk_marker = 7

        dt_should_be = datetime(2000, 1, 2, 0, 0, 0)
        with mock.patch.object(uuid, 'uuid4', return_value='UUID-HERE'):
            storage_key = cold_storage.store({'data': 'yeah!'}, job_scope, chunk_marker=7)

        prefix = xxhash.xxh64(job_scope.ad_account_id).hexdigest()[:6]
        expected_key = (
            f'fb/'
            + f'{prefix}-{job_scope.ad_account_id}/'
            + f'{job_scope.report_type}/'
            + f'{dt_should_be.strftime("%Y")}/'
            + f'{dt_should_be.strftime("%m")}/'
            + f'{dt_should_be.strftime("%d")}/'
            + f'{dt_should_be.strftime("%Y-%m-%dT%H:%M:%SZ")}-'
            + f'{job_scope.job_id}-'
            + f'{chunk_marker}-'
            + f'UUID-HERE'
            + f'.json'
        )

        assert storage_key == expected_key


def test__job_scope_to_metadata():
    scope = JobScope(
        job_id='job identifier',
        namespace='fb',
        ad_account_id='007',
        report_type='report type',
        entity_type=Entity.Campaign,
        report_variant=Entity.Ad,
        range_start=datetime.fromtimestamp(1),
        score=10,
    )

    result = _job_scope_to_metadata(scope)

    result.pop('extracted_at')
    result.pop('build_id')

    assert {
        'job_id': 'fb|007|C||report+type|A|1970-01-01T00%3A00%3A01',
        'ad_account_id': '007',
        'report_type': 'report type',
        'entity_type': 'A',
        'platform_api_version': 'v4.0',
        'platform': 'fb',
        'score': '10',
    } == result
