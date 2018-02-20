from facebookads.adobjects import (
    ad,
    adaccount,
    adset,
    campaign
)

from common.enums.entity import Entity


ENTITY_ADACCOUNT = adaccount.AdAccount
ENTITY_CAMPAIGN = campaign.Campaign
ENTITY_ADSET = adset.AdSet
ENTITY_AD = ad.Ad

ENTITY_NAMES = {
    ENTITY_ADACCOUNT: Entity.AdAccount,
    ENTITY_CAMPAIGN: Entity.Campaign,
    ENTITY_ADSET: Entity.AdSet,
    ENTITY_AD: Entity.Ad
}
