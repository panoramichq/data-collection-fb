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

from common.store import entities


def iter_entities_per_ad_account_id(ad_account_id, fields=None):
    """
    :return: A generator of yielding data for all children of given AdAccounts
    :rtype: Generator[Dict]
    """

    entity_models = [
        entities.FacebookCampaignEntity,
        entities.FacebookAdsetEntity,
        entities.FacebookAdEntity,
    ]

    for EntityModel in entity_models:
        for record in EntityModel.query(ad_account_id):
            yield record.to_dict(fields=fields, skip_null=True)
