from datetime import datetime

from tests.base.testcase import TestCase, integration

from common.enums.entity import Entity
from config.facebook import TOKEN, AD_ACCOUNT
from oozer.common.job_scope import JobScope
from oozer.common.facebook_api import PlatformApiContext
from oozer.common.job_context import JobContext
from oozer.entities.collect_entities_per_adaccount import \
    iter_collect_entities_per_adaccount, iter_native_entities_per_adaccount


@integration('facebook')
class TestingEntityCollection(TestCase):

    def test_fetch_all_campaigns(self):

        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(
                ad_account,
                Entity.Campaign
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_adsets(self):

        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(
                ad_account,
                Entity.AdSet
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ads(self):

        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(
                ad_account,
                Entity.Ad
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ad_creatives(self):
        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(
                ad_account,
                Entity.AdCreative
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ad_videos(self):
        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(
                ad_account,
                Entity.AdVideo
            )
            cnt = 0
            for entity in entities:
                assert entity['account_id'] == AD_ACCOUNT  # This tests if we're augmenting correctly
                cnt += 1
                break

            assert cnt


class TestingEntityCollectionPipeline(TestCase):
    @integration('facebook')
    def test_pipeline_campaigns(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entities',
            report_variant=Entity.Campaign,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(
            job_scope, JobContext()
        )

        cnt = 0
        for datum in data_iter:
            cnt += 1
            if cnt == 4:
                break

        assert cnt == 4

    @integration('facebook')
    def test_pipeline_creatives(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entities',
            report_variant=Entity.AdCreative,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(
            job_scope, JobContext()
        )

        cnt = 0
        for datum in data_iter:
            cnt += 1
            if cnt == 4:
                break

        assert cnt == 4

    @integration('facebook')
    def test_pipeline_ad_videos(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entities',
            report_variant=Entity.AdVideo,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(
            job_scope, JobContext()
        )

        cnt = 0
        for datum in data_iter:
            cnt += 1
            break

        assert cnt
