from tests.base.testcase import TestCase, integration

from unittest import skip
from pprint import pprint
from twitter_ads.client import Client

from common.twitter.enums.entity import Entity
from oozer.common.twitter_api import TwitterApiContext
from oozer.entities.twitter.collect_entities_per_adaccount import iter_native_entities_per_adaccount

from config.twitter import CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET, AD_ACCOUNT



@integration('twitter')
class TestingEntityCollection(TestCase):


    def test_fetch_campaigns_per_account(self):
        client = Client(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET)

        import ipdb; ipdb.set_trace()

        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as ctx:
            ad_account = ctx.to_tw_model(AD_ACCOUNT, Entity.Account)

            count = 0
            for campaign in iter_native_entities_per_adaccount(ad_account, Entity.Campaign):
                count += 1
                pprint(campaign)
                import ipdb; ipdb.set_trace()
                break

            assert count

    def test_fetch_line_items_per_account(self):
        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as ctx:
            ad_account = ctx.to_tw_model(AD_ACCOUNT, Entity.Account)

            count = 0
            for line_item in iter_native_entities_per_adaccount(ad_account, Entity.LineItem):
                count += 1
                pprint(line_item)
                import ipdb; ipdb.set_trace()
                break

            assert count

    def test_fetch_promoted_tweets_per_account(self):
        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as ctx:
            ad_account = ctx.to_tw_model(AD_ACCOUNT, Entity.Account)

            count = 0
            for promoted_tweet in iter_native_entities_per_adaccount(ad_account, Entity.PromotedTweet):
                count += 1
                import ipdb; ipdb.set_trace()
                pprint(promoted_tweet)
                break

            assert count


@skip
class TestingEntityCollectionPipeline(TestCase):

    @integration('twitter')
    def test_pipeline(self):
        assert 1 == 0, 'Not implemented'
