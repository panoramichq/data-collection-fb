from typing import Generator

from common.enums.entity import Entity
from common.store import entities

from .adaccounts import iter_scopes_tokens, iter_active_ad_accounts_per_scope
from .entities import iter_entities_per_ad_account_id
from .reality_claim import RealityClaim

from config.operam_console_api import TOKEN as CONSOLE_API_TOKEN


def iter_reality():
    """
    A generator yielding instances of RealityClaim object, filled
    with data about some entity (one of AdAccount, Campaign, AdSet, Ad)

    :return: Generator yielding RealityClaim objects pertaining to various levels of entities
    :rtype: Generator[RealityClaim]
    """
    for scope, tokens in iter_scopes_tokens():
        # For every "scope" there is yield we should update data from there
        # In this case there is just one 'Console' scope
        # This claim has its own collection task that queries the console API
        # and updates the AdAccount records in dynamo
        yield RealityClaim(
            scope=scope,
            entity_type=Entity.AdAccount,
            tokens=[CONSOLE_API_TOKEN]
        )

        ad_account = None  # type: entities.FacebookAdAccountEntity
        for ad_account in iter_active_ad_accounts_per_scope(scope):
            assert ad_account.timezone  # we need the timezone to align insights properly

            # FIXME: There isn't a worker for this at the moment, everything gets fetched from the console API
            # AdAccount level objects have their own data collection tasks
            # we always give them to prioritizer
            yield RealityClaim(
                ad_account_id=ad_account.ad_account_id,
                entity_id=ad_account.ad_account_id,
                entity_type=Entity.AdAccount,
                timezone=ad_account.timezone,
                tokens=tokens
            )

            # now we need to spit out each of AA's children entities
            for entity_data in iter_entities_per_ad_account_id(ad_account.ad_account_id):
                yield RealityClaim(
                    entity_data,
                    timezone=ad_account.timezone,
                    tokens=tokens
                )
