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


def iter_scopes():
    # type: () -> Generator[scope.AssetScope]
    """
    :return: a generator of pairs of: tuple of scope id and its associated set of FB tokens
    :rtype: Generator[scope.AssetScope]
    """
    yield from scope.AssetScope.scan()

def iter_active_pages_per_scope(scope):
    # type: (str) -> Generator[entities.PageEntity]
    """
    :param str scope: The PageScope id
    :return: A generator of Page IDs for Pages marked "active" in our system
    :rtype: Generator[entities.PageEntity]
    """
    page_record = None  # type: entities.PageEntity
    for page_record in entities.PageEntity.query(scope):
        if page_record.is_active:
            yield page_record
