"""
This module is here to runtime-patch problems with Twitter SDK or API
"""

def patch_twitter_sdk():
    from twitter_ads.resource import Resource
    from twitter_ads.account import Account
    from twitter_ads.campaign import Campaign, LineItem
    from twitter_ads.creative import PromotedTweet

    def account_promoted_tweets(self, id=None, **kwargs):
        """
        Returns a collection of promotable tweets available to the
        current account.
        """
        return self._load_resource(PromotedTweet, id, **kwargs)

    def campaign_line_items(self, **kwargs):
        return LineItem.all(self.account, campaign_ids=[self.id], **kwargs)

    def line_item_promoted_tweets(self, **kwargs):
        return PromotedTweet.all(self.account, line_item_ids=[self.id], **kwargs)


    Account.promoted_tweets = account_promoted_tweets
    Campaign.line_items = campaign_line_items
    LineItem.promoted_tweets = line_item_promoted_tweets

    def resource_to_dict(self):
        params = self.to_params()

        if self.__class__ in [Campaign, LineItem, PromotedTweet]:
            params['account_id'] = self.account.id

        return params

    Resource.to_dict = resource_to_dict
