# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

FB_AD_ACCOUNT_ENTITY_TABLE = 'AdAccountEntity'
FB_AD_ACCOUNT_SCOPE_TABLE = 'AdAccountScope'
FB_AD_ENTITY_TABLE = 'AdEntity'
FB_ADSET_ENTITY_TABLE = 'AdsetEntity'
FB_CAMPAIGN_ENTITY_TABLE = 'CampaignEntity'
FB_TOKEN_TABLE = 'PlatformToken'
JOB_REPORT_TABLE = 'JobReport'

from common.updatefromenv import update_from_env
update_from_env(__name__)
