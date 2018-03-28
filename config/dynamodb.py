# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

AD_ACCOUNT_ENTITY_TABLE = 'FacebookAdAccountEntity'
AD_ACCOUNT_SCOPE_TABLE = 'FacebookAdAccountScope'
AD_ENTITY_TABLE = 'FacebookAdEntity'
ADSET_ENTITY_TABLE = 'FacebookAdsetEntity'
CAMPAIGN_ENTITY_TABLE = 'FacebookCampaignEntity'
FB_TOKEN_TABLE = 'FacebookPlatformToken'
JOB_REPORT_TABLE = 'JobReport'

from common.updatefromenv import update_from_env
update_from_env(__name__)
