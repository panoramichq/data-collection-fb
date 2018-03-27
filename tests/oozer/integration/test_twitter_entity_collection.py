from tests.base.testcase import TestCase, integration

from datetime import datetime
from unittest import skip
from pprint import pprint

from common.twitter.enums.entity import Entity
from oozer.common.job_scope import JobScope
from oozer.common.job_context import JobContext
from oozer.common.twitter_api import TwitterApiContext
from oozer.entities.twitter.collect_entities_per_adaccount import iter_native_entities_per_adaccount, \
    iter_collect_entities_per_adaccount

from config.twitter import CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET, AD_ACCOUNT


@integration('twitter')
class TestingNativeEntityCollection(TestCase):

    def test_fetch_campaigns_per_account(self):
        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as ctx:
            ad_account = ctx.to_tw_model(AD_ACCOUNT, Entity.AdAccount)

            count = 0
            for campaign in iter_native_entities_per_adaccount(ad_account, Entity.Campaign):
                print(campaign.id)
                count += 1
                break

            assert count

    def test_fetch_line_items_per_account(self):
        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as ctx:
            ad_account = ctx.to_tw_model(AD_ACCOUNT, Entity.AdAccount)

            count = 0
            for line_item in iter_native_entities_per_adaccount(ad_account, Entity.LineItem):
                count += 1

                break

            assert count

    def test_fetch_promoted_tweets_per_account(self):
        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as ctx:
            ad_account = ctx.to_tw_model(AD_ACCOUNT, Entity.AdAccount)

            count = 0
            for promoted_tweet in iter_native_entities_per_adaccount(ad_account, Entity.PromotedTweet):
                count += 1
                break

            assert count


@integration('twitter')
class TestingEntityCollectionPipeline(TestCase):

    def test_pipeline_for_campaigns(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            report_time=datetime.utcnow(),
            report_type='entities',
            report_variant=Entity.Campaign,
            sweep_id='test_1'
        )

        data_iter = iter_collect_entities_per_adaccount(
            job_scope,
            JobContext()
        )

        cnt = 0
        for datum in data_iter:
            cnt += 1
            pprint(datum)
            if cnt == 4:
                break

        assert cnt

    def test_pipeline_for_line_items(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            report_time=datetime.utcnow(),
            report_type='entities',
            report_variant=Entity.LineItem,
            sweep_id='test_1'
        )

        data_iter = iter_collect_entities_per_adaccount(
            job_scope,
            JobContext()
        )

        cnt = 0
        for datum in data_iter:
            cnt += 1
            pprint(datum)
            if cnt == 4:
                break

        assert cnt

    def test_pipeline_for_promoted_tweets(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            report_time=datetime.utcnow(),
            report_type='entities',
            report_variant=Entity.PromotedTweet,
            sweep_id='test_1'
        )

        data_iter = iter_collect_entities_per_adaccount(
            job_scope,
            JobContext()
        )

        cnt = 0
        for datum in data_iter:
            cnt += 1
            pprint(datum)
            if cnt == 4:
                break

        assert cnt
