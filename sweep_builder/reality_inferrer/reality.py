from typing import Generator, List

from common.enums.entity import Entity
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.reality_inferrer.pages import iter_active_pages_per_scope

from sweep_builder.reality_inferrer.adaccounts import iter_scopes, iter_active_ad_accounts_per_scope
from sweep_builder.reality_inferrer.entities import iter_entities_per_ad_account_id, iter_entities_per_page_id


def iter_reality_base() -> Generator[RealityClaim, None, None]:
    """
    A generator yielding instances of RealityClaim object, filled
    with data about some entity (one of Scope, AdAccount)

    These claims represent our knowledge about the what exists.

    Some consuming code will match these claims of existence to tasks
    we are expected to perform for these objects.

    :return: Generator yielding RealityClaim objects pertaining to various levels of entities
    """
    # Scopes are top-level objects in our system
    # they are like binders into which we put some top-level platform assets
    # and their associated stuff (like tokens etc)

    for scope_record in iter_scopes():

        # Each of the scopes has their own refresh tasks
        # These are triggered by our claim of existence of the scope
        # For example, reaching out to console API
        # to refresh the list of AdAccount records

        # This scope exists claim:
        yield RealityClaim(entity_id=scope_record.scope, entity_type=Entity.Scope)

        # For all the ad accounts we already know are attached to the scope
        # we need to kick of their refresh tasks, thus,
        # yielding out existence claims pertaining to AdAccounts per scope

        for ad_account in iter_active_ad_accounts_per_scope(scope_record.scope):

            # This reality claim is base for expectation to have
            # its children lists refreshed.
            yield RealityClaim(
                ad_account_id=ad_account.ad_account_id,
                entity_id=ad_account.ad_account_id,
                entity_type=Entity.AdAccount,
                timezone=ad_account.timezone,
            )

        for page in iter_active_pages_per_scope(scope_record.scope):
            yield RealityClaim(ad_account_id=page.page_id, entity_id=page.page_id, entity_type=Entity.Page)


def iter_reality_per_ad_account_claim(
    ad_account_claim: RealityClaim, entity_types: List[str] = None
) -> Generator[RealityClaim, None, None]:
    """
    A generator yielding instances of RealityClaim object, filled
    with data about some entity (one of Campaign, AdSet, Ad)

    These claims represent our knowledge about the what exists.

    Some consuming code will match these claims of existence to tasks
    we are expected to perform for these objects.

    :param ad_account_claim: A RealityClaim instance representing existence of AdAccount
    :param  entity_types: If truethy, limits the reality iterator to those types of entities only.
    :return: Generator yielding RealityClaim objects pertaining to various levels of entities
    """
    # Naturally, we may know about some of the AdAccount's children
    # existing already and might need their supporting data refreshed too.
    for entity_data in iter_entities_per_ad_account_id(ad_account_claim.ad_account_id, entity_types=entity_types):
        yield RealityClaim(entity_data, timezone=ad_account_claim.timezone)


def iter_reality_per_page_claim(
    page_claim: RealityClaim, entity_types: List[str] = None
) -> Generator[RealityClaim, None, None]:
    """
    A generator yielding instances of RealityClaim object, filled
    with data about some entity (one of Campaign, AdSet, Ad)

    These claims represent our knowledge about the what exists.

    Some consuming code will match these claims of existence to tasks
    we are expected to perform for these objects.

    :param page_claim: A RealityClaim instance representing existence of Page
    :param entity_types: If truthy, limits the reality iterator to those types of entities only.
    :return: Generator yielding RealityClaim objects pertaining to various levels of entities
    """
    for entity_data in iter_entities_per_page_id(page_claim.entity_id, page_entity_types=entity_types):
        yield RealityClaim(entity_data)
