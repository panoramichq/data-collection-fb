from common.enums.entity import Entity
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.customaudience import CustomAudience


ENTITY_TYPE_MODEL_MAP = {
    Entity.Ad: Ad,
    Entity.AdAccount: AdAccount,
    Entity.AdSet: AdSet,
    Entity.Campaign: Campaign,
    Entity.AdCreative: AdCreative,
    Entity.AdVideo: AdVideo,
    Entity.CustomAudience: CustomAudience,
}


MODEL_ENTITY_TYPE_MAP = {
    value: key
    for key, value in ENTITY_TYPE_MODEL_MAP.items()
}
