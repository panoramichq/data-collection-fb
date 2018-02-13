# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

FB_CAMPAIGN_ENTITY_TABLE = 'FacebookCampaignEntity'
FB_ADSET_ENTITY_TABLE = 'FacebookAdsetEntity'
FB_AD_ENTITY_TABLE = 'FacebookAdEntity'
FB_ENTITY_REPORT_TYPE_TABLE = 'EntityReportType'
FB_SWEEP_ENTITY_REPORT_TYPE_TABLE = 'SweepEntityReportType'

from common.updatefromenv import update_from_env
update_from_env(__name__)
