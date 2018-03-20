from twitter_ads.cursor import Cursor

from common.twitter.enums.entity import Entity


def iter_native_entities_per_adaccount(ad_account, entity_type):
    """
    Generic getter for entities from the AdAccount edge

    :param AdAccount ad_account: Ad account id
    :param str entity_type:
    :param list fields: List of entity fields to fetch
    :return Cursor:
    """

    if entity_type not in Entity.ALL:
        raise ValueError(
            f'Value of "entity_type" argument must be one of {Entity.ALL}, '
            f'got {entity_type} instead.'
        )

    getter_method = {
        Entity.Campaign: ad_account.campaigns,
        Entity.LineItem: ad_account.line_items,
        Entity.PromotedTweet: ad_account.promoted_tweets
    }[entity_type]

    yield from getter_method()


def iter_collect_entities_per_adaccount(job_scope, job_context):
    pass
