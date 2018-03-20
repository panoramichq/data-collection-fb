from twitter_ads.account import Account
from twitter_ads.campaign import Campaign, LineItem
from twitter_ads.creative import PromotedTweet

from common.twitter.enums.entity import Entity

ENTITY_TYPE_MODEL_MAP = {
    Entity.AdAccount: Account,
    Entity.Campaign: Campaign,
    Entity.LineItem: LineItem,
    Entity.PromotedTweet: PromotedTweet
}


MODEL_ENTITY_TYPE_MAP = {
    value: key
    for key, value in ENTITY_TYPE_MODEL_MAP.items()
}
