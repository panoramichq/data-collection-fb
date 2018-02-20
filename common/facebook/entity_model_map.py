from common.enums.entity import Entity
from facebookads.adobjects.ad import Ad
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.campaign import Campaign


ENTITY_TYPE_MODEL_MAP = {
    Entity.Ad: Ad,
    Entity.AdAccount: AdAccount,
    Entity.AdSet: AdSet,
    Entity.Campaign: Campaign,
}


MODEL_ENTITY_TYPE_MAP = {
    value: key
    for key, value in ENTITY_TYPE_MODEL_MAP.items()
}
