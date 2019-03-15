from datetime import datetime

from oozer.entities.collect_entities_iterators import (
    iter_native_entities_per_adaccount,
    iter_native_entities_per_page,
    iter_collect_entities_per_adaccount,
    iter_collect_entities_per_page,
)
from tests.base.testcase import TestCase, integration

from common.enums.entity import Entity
from config.facebook import TOKEN, AD_ACCOUNT, PAGE
from oozer.common.job_scope import JobScope
from oozer.common.facebook_api import PlatformApiContext


@integration('facebook')
class TestingEntityCollection(TestCase):
    def test_fetch_all_campaigns(self):

        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(ad_account, Entity.Campaign)
            cnt = 0
            for _ in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_adsets(self):

        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(ad_account, Entity.AdSet)
            cnt = 0
            for _ in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ads(self):

        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(ad_account, Entity.Ad)
            cnt = 0
            for _ in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ad_creatives(self):
        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(ad_account, Entity.AdCreative)
            cnt = 0
            for _ in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ad_videos(self):
        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            entities = iter_native_entities_per_adaccount(ad_account, Entity.AdVideo)
            cnt = 0
            for entity in entities:
                assert entity['account_id'] == AD_ACCOUNT  # This tests if we're augmenting correctly
                cnt += 1
                break

            assert cnt

    def test_fetch_all_custom_audiences(self):
        with PlatformApiContext(TOKEN) as ctx:
            ad_account = ctx.to_fb_model(AD_ACCOUNT, Entity.AdAccount)
            entities = iter_native_entities_per_adaccount(ad_account, Entity.CustomAudience)
            cnt = 0

            for _ in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_page_posts(self):
        with PlatformApiContext(TOKEN) as ctx:
            page = ctx.to_fb_model(PAGE, Entity.Page)
            entities = iter_native_entities_per_page(page, Entity.PagePost)
            cnt = 0

            for _ in entities:
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
            report_type='entity',
            report_variant=Entity.Campaign,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(job_scope)

        cnt = 0
        for _ in data_iter:
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
            report_type='entity',
            report_variant=Entity.AdCreative,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(job_scope)

        cnt = 0
        for _ in data_iter:
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
            report_type='entity',
            report_variant=Entity.AdVideo,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(job_scope)

        cnt = 0
        for _ in data_iter:
            cnt += 1
            break

        assert cnt

    @integration('facebook')
    def test_pipeline_custom_audiences(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.CustomAudience,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_adaccount(job_scope)

        cnt = 0
        for _ in data_iter:
            cnt += 1
            break

        assert cnt

    @integration('facebook')
    def test_pipeline_page_posts(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.PagePost,
            sweep_id='1'
        )

        data_iter = iter_collect_entities_per_page(job_scope)

        cnt = 0
        for _ in data_iter:
            cnt += 1
            break

        assert cnt
