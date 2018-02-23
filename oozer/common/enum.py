from facebookads.adobjects import (
    ad,
    adaccount,
    adset,
    campaign
)

from common.enums.entity import Entity


FB_ADACCOUNT_MODEL = adaccount.AdAccount
FB_CAMPAIGN_MODEL = campaign.Campaign
FB_ADSET_MODEL = adset.AdSet
FB_AD_MODEL = ad.Ad

FB_MODEL_ENUM_VALUE_MAP = {
    FB_ADACCOUNT_MODEL: Entity.AdAccount,
    FB_CAMPAIGN_MODEL: Entity.Campaign,
    FB_ADSET_MODEL: Entity.AdSet,
    FB_AD_MODEL: Entity.Ad
}

ENUM_VALUE_FB_MODEL_MAP = {
    value: Model
    for Model, value in FB_MODEL_ENUM_VALUE_MAP.items()
}


def to_fb_model(entity_id, entity_type, api=None):
    assert entity_type in Entity.ALL

    if entity_type == Entity.AdAccount:
        fbid = f'act_{entity_id}'
    else:
        fbid = entity_id

    return ENUM_VALUE_FB_MODEL_MAP[entity_type](fbid=fbid, api=api)
