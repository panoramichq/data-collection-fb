from typing import Generator

from common.enums.entity import Entity

from .adaccounts import iter_ad_account_id_tz_tokens, iter_scopes_tokens
from .entities import iter_entities_per_ad_account_id
from .reality_claim import RealityClaim


def iter_reality():
    """
    A generator yielding instances of RealityClaim object, filled
    with data about some entity (one of AdAccount, Campaign, AdSet, Ad)

    :return: Generator yielding RealityClaim objects pertaining to various levels of entities
    :rtype: Generator[RealityClaim]
    """
    for scope, token in iter_scopes_tokens():
        # For every "scope" there is yield we should update data from there
        # In this case there is just one 'Console' scope
        # This claim has its own collection task that queries the console API
        # and updates the AdAccount records in dynamo
        yield RealityClaim(
            scope=scope,
            entity_type=Entity.AdAccount,
            tokens=['DUMMY']  # FIXME: replace with a token used to auth against the console API
        )

    for ad_account_id, timezone, tokens in iter_ad_account_id_tz_tokens():
        # AdAccount level objects have their own data collection tasks
        # we always give them to prioritizer
        yield RealityClaim(
            ad_account_id=ad_account_id,
            entity_id=ad_account_id,
            entity_type=Entity.AdAccount,
            timezone=timezone,
            tokens=tokens
        )

        # TODO if not timezone dont run entity fetch

        # now we need to spit out each of AA's children entities
        for entity_data in iter_entities_per_ad_account_id(ad_account_id):
            yield RealityClaim(
                entity_data,
                timezone=timezone,
                tokens=tokens
            )
