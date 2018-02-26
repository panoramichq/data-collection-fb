from typing import Generator

from common.enums.entity import Entity

from .adaccounts import iter_ad_account_id_tz_tokens
from .entities import iter_entities_per_ad_account_id
from .reality_claim import RealityClaim


def iter_reality():
    """
    A generator yielding instances of RealityClaim object, filled
    with data about some entity (one of AdAccount, Campaign, AdSet, Ad)

    :return: Generator yielding RealityClaim objects pertaining to various levels of entities
    :rtype: Generator[RealityClaim]
    """

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

        # now we need to spit out each of AA's children entities
        for entity_data in iter_entities_per_ad_account_id(ad_account_id):
            yield RealityClaim(
                entity_data,
                timezone=timezone,
                tokens=tokens
            )
