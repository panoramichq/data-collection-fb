"""
This module's code is responsible for understanding 2 things:
1. Based on our internal records figure out what entities we know to exist
2. Based on our internal records figure some basic facts about existence
   of the entities: beginning of life, end of life (if applicable)

Code of this module is NOT concerned with flavors of data collection tasks
or their prior success. We are just looking at what objects we will eventually
need to fetch supporting data for.

NO CALLS ARE MADE TO EXTERNAL (outside of this system) SERVICES
(like Facebook, or Operam Console etc). All external data collection
calls are done by distributed Data Collection Workers somewhere else.
Here we base our understanding of the world based on scraps of data
these workers already collected some time before.
"""

from typing import Generator

from common.store import entities, scope


def iter_scopes() -> Generator[scope.AssetScope, None, None]:
    """
    :return: a generator of pairs of: tuple of scope id and its associated set of FB tokens
    """
    # when we get real API that pairs AAs to their tokens,
    # throw all of this away

    # .query() on AdAccountEntity does not hint well at type
    # have do to it manually for IDE to pick it up
    yield from scope.AssetScope.scan()


def iter_active_ad_accounts_per_scope(scope: str) -> Generator[entities.AdAccountEntity, None, None]:
    """
    :param scope: The AdAccountScope id
    :return: A generator of AdAccount IDs for AdAccounts marked "active" in our system
    """
    for aa_record in entities.AdAccountEntity.query(scope):
        # note that we can filter by this server-side,
        # but this involves setting up an index on the partition,
        # which may limit the size of the partition.
        # TODO: investigate the risk and move this filter DB-side
        if aa_record.is_active and aa_record.manually_disabled is not True:
            yield aa_record
