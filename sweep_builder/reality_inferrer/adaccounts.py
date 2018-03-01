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

from typing import Generator, Tuple, Set

from common.store import entities, scope


def iter_scopes_tokens():
    """
    :return: a generator of pairs of: tuple of scope id and its associated set of FB tokens
    :rtype: Generator[Tuple[str, Set[str]]]
    """
    # when we get real API that pairs AAs to their tokens,
    # throw all of this away

    # .query() on FacebookAdAccountEntity does not hint well at type
    # have do to it manually for IDE to pick it up
    scope_record = None  # type: scope.FacebookAdAccountScope

    for scope_record in scope.FacebookAdAccountScope.scan():
        yield scope_record.scope, scope_record.tokens


def _iter_active_ad_account_per_scope(scope):
    """
    :return: A generator of AdAccount IDs for AdAccounts marked "active" in our system
    :rtype: Generator[Tuple[str, Set[str]]]
    """

    # .query() on FacebookAdAccountEntity does not hint well at type
    # have do to it manually for IDE to pick it up
    aa_record = None  # type: entities.FacebookAdAccountEntity

    for aa_record in entities.FacebookAdAccountEntity.query(scope):
        # note that we can filter by this server-side,
        # but this involves setting up an index on the partition,
        # which may limit the size of the partition.
        # TODO: investigate the risk and move this filter DB-side
        if aa_record.is_active:
            yield aa_record


def iter_ad_account_id_tz_tokens():
    """
    Public API

    :return: A a generator yielding pairs of: ad_account_id and its associated set of platform tokens
    :rtype: Generator[Tuple[str, str, Set[str]]]
    """
    for scope, tokens in _iter_scopes_tokens():
        if tokens:
            for record in _iter_active_ad_account_per_scope(scope):
                yield record.ad_account_id, record.timezone, tokens
