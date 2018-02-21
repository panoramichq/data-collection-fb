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
