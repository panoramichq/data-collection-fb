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

from typing import Generator, List, Any, Dict

from common.enums.entity import Entity
from common.store import entities
from common.measurement import Measure

entity_type_model_map = {
    Entity.Campaign: entities.CampaignEntity,
    Entity.AdSet: entities.AdsetEntity,
    Entity.Ad: entities.AdEntity,
    Entity.AdCreative: entities.AdCreativeEntity,
    Entity.AdVideo: entities.AdVideoEntity,
    Entity.CustomAudience: entities.CustomAudienceEntity,
}

page_entity_type_model_map = {Entity.PagePost: entities.PagePostEntity, Entity.PageVideo: entities.PageVideoEntity}


def iter_entities_per_ad_account_id(
    ad_account_id: str, fields: List[str] = None, entity_types: List[str] = None
) -> Generator[Dict[str, Any], None, None]:
    # occasionally it's important to pass through
    # we are not overriding the values, but must pass some value
    # state in entity_models
    # There we treat explicit None, or empty array as "use default list"

    if not entity_types:
        # All types are returned
        entity_models = entity_type_model_map.values()
    else:
        # intentionally leaving this logic brittle
        # this function is linked to types "statically"
        # and is not expected to hide misses in the map.
        entity_models = [entity_type_model_map[entity_type] for entity_type in entity_types]

    _step = 1000

    for EntityModel in entity_models:
        cnt = 0

        with Measure.counter(
            __name__ + '.entities_per_ad_account_id',
            tags={'ad_account_id': ad_account_id, 'entity_type': EntityModel.entity_type},
        ) as cntr:

            for record in EntityModel.query(ad_account_id):
                cnt += 1
                yield record.to_dict(fields=fields, skip_null=True)
                if cnt % _step == 0:
                    cntr += _step

            if cnt % _step:
                cntr += cnt % _step


def iter_entities_per_page_id(
    page_id: str, fields: List[str] = None, page_entity_types: List[str] = None
) -> Generator[Dict[str, Any], None, None]:
    if not page_entity_types:
        page_entity_models = page_entity_type_model_map.values()
    else:
        page_entity_models = [page_entity_type_model_map[entity_type] for entity_type in page_entity_types]

    _step = 1000

    for EntityModel in page_entity_models:
        cnt = 0

        with Measure.counter(
            __name__ + '.entities_per_page_id', tags={'ad_account_id': page_id, 'entity_type': EntityModel.entity_type}
        ) as cntr:

            for record in EntityModel.query(page_id):
                cnt += 1
                record_dict = record.to_dict(fields=fields, skip_null=True)
                # this is unfortunate, but we need to change page_id to ad_account_id
                record_dict['ad_account_id'] = record_dict['page_id']
                del record_dict['page_id']
                yield record_dict
                if cnt % _step == 0:
                    cntr += _step

            if cnt % _step:
                cntr += cnt % _step
