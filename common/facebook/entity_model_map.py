from common.enums.entity import Entity
from facebookads.adobjects.ad import Ad
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.campaign import Campaign
from facebookads.adobjects.adcreative import AdCreative
from facebookads.adobjects.advideo import AdVideo


ENTITY_TYPE_MODEL_MAP = {
    Entity.Ad: Ad,
    Entity.AdAccount: AdAccount,
    Entity.AdSet: AdSet,
    Entity.Campaign: Campaign,
    Entity.AdCreative: AdCreative,
    Entity.AdVideo: AdVideo,
}


MODEL_ENTITY_TYPE_MAP = {
    value: key
    for key, value in ENTITY_TYPE_MODEL_MAP.items()
}
