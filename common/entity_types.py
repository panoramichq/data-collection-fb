"""
Maintains a mapping between FB entity objects and string representations we use
(for serialization and storage)
"""
from facebookads.adobjects import campaign, adset, ad
from .store import entities

ENTITY_CAMPAIGN = campaign.Campaign
ENTITY_ADSET = adset.AdSet
ENTITY_AD = ad.Ad

ENTITY_NAMES = {
    ENTITY_CAMPAIGN: 'C',
    ENTITY_ADSET: 'AS',
    ENTITY_AD: 'A'
}

ENTITY_TYPES = {v: k for k, v in ENTITY_NAMES.items()}


def get_entity_name_from_fb_object(facebook_object):
    """
    Translates a facebook object instance to our representation of an entity
    type (e.g. A/AS etc.)

    :param object facebook_object:
    :return string: The abbreviated entity type
    """
    return ENTITY_NAMES[facebook_object.__class__]


def get_entity_model_from_entity_name(entity_name):
    """
    Translate our abbreviated entity type name (e.g. AS) to the entity table
    model class

    :param string entity_name: The abbreviated entity name identifier (C/AS/A)
    :return BaseModel: The model class
    """
    # Sanity check entity_type
    assert entity_name in ENTITY_NAMES.values()

    klazz = {
        ENTITY_NAMES[ENTITY_CAMPAIGN]: entities.FacebookCampaignEntity,
        ENTITY_NAMES[ENTITY_ADSET]: entities.FacebookAdsetEntity,
        ENTITY_NAMES[ENTITY_AD]: entities.FacebookAdEntity,
    }[entity_name]

    return klazz
