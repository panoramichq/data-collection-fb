# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

AD_ACCOUNT_ENTITY_TABLE = 'b3fb5e-datacol-AD_ACCOUNT_ENTITY_TABLE'
AD_ACCOUNT_SCOPE_TABLE = 'b3fb5e-datacol-AD_ACCOUNT_SCOPE_TABLE'
AD_ENTITY_TABLE = 'b3fb5e-datacol-AD_ENTITY_TABLE'
AD_CREATIVE_ENTITY_TABLE = 'b3fb5e-datacol-AD_CREATIVE_ENTITY_TABLE'
CUSTOM_AUDIENCE_ENTITY_TABLE = 'b3fb5e-datacol-CUSTOM_AUDIENCE_ENTITY_TABLE'
AD_VIDEO_ENTITY_TABLE = 'b3fb5e-datacol-AD_VIDEO_ENTITY_TABLE'
ADSET_ENTITY_TABLE = 'b3fb5e-datacol-ADSET_ENTITY_TABLE'
CAMPAIGN_ENTITY_TABLE = 'b3fb5e-datacol-CAMPAIGN_ENTITY_TABLE'
TOKEN_TABLE = 'b3fb5e-datacol-TOKEN_TABLE'
JOB_REPORT_TABLE = 'b3fb5e-datacol-JOB_REPORT_TABLE'

from common.updatefromenv import update_from_env
update_from_env(__name__)
