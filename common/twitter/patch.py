"""
This module is here to runtime-patch problems with Twitter SDK or API
"""


from twitter_ads.account import Account
from twitter_ads.creative import PromotedTweet

def account_promoted_tweets(self, id=None, **kwargs):
    """
    Returns a collection of promotable users available to the
    current account.
    """
    return self._load_resource(PromotedTweet, id, **kwargs)

Account.promoted_tweets = account_promoted_tweets
