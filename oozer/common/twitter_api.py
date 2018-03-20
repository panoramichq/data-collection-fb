from twitter_ads.client import Client

from twitter_ads import (
    account,
    campaign,
    creative
)


from common.twitter.enums.entity import Entity

TW_ADACCOUNT_MODEL = account.Account
TW_CAMPAIGN_MODEL = campaign.Campaign
TW_LINE_ITEM_MODEL = campaign.LineItem
TW_PROMOTED_TWEET_MODEL = creative.PromotedTweet


class TwitterApiContext:
    """
    A simple wrapper for Facebook SDK, using local API sessions as not to
    pollute the the global default API session with initialization
    """

    token = None  # type: str
    client = None  # type: Client

    def __init__(self, consumer_key, consumer_secret, token, secret):
        """
        :param token:
        """
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.secret = secret
        self.token = token

    def __enter__(self):
        """

        """
        self.client = Client(self.consumer_key, self.consumer_secret, self.token, self.secret)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        We do not need to do anything specific yet
        """
        pass

    def to_tw_model(self, entity_id, entity_type):
        """
        Like stand-alone to_fb_model but removes the need to pass in API
        instance manually

        :param string entity_id: The entity ID
        :param entity_type:
        :return:
        """

        entity_constructor = {
            Entity.Account: self.client.accounts,
            Entity.Campaign: TW_ADACCOUNT_MODEL(client=self.client).campaigns,
            Entity.LineItem: TW_ADACCOUNT_MODEL(client=self.client).line_items,
            Entity.PromotedTweet: TW_ADACCOUNT_MODEL(client=self.client).promoted_tweets
        }

        return entity_constructor[entity_type](entity_id)
