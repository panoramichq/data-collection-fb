# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

AD_ACCOUNT_ENTITY_TABLE = 'AdAccountEntity'
AD_ACCOUNT_SCOPE_TABLE = 'AdAccountScope'
AD_ENTITY_TABLE = 'AdEntity'
AD_CREATIVE_ENTITY_TABLE = 'AdCreativeEntity'
CUSTOM_AUDIENCE_ENTITY_TABLE = 'CustomAudienceEntity'
AD_VIDEO_ENTITY_TABLE = 'AdVideoEntity'
ADSET_ENTITY_TABLE = 'AdsetEntity'
CAMPAIGN_ENTITY_TABLE = 'CampaignEntity'
PAGE_ENTITY_TABLE = 'PageEntity'
PAGE_POST_ENTITY_TABLE = 'PagePostEntity'
TOKEN_TABLE = 'PlatformToken'
JOB_REPORT_TABLE = 'JobReport'

from common.updatefromenv import update_from_env
update_from_env(__name__)
