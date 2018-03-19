# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

FB_AD_ACCOUNT_ENTITY_TABLE = 'FacebookAdAccountEntity'
FB_AD_ACCOUNT_SCOPE_TABLE = 'FacebookAdAccountScope'
FB_AD_ENTITY_TABLE = 'FacebookAdEntity'
FB_ADSET_ENTITY_TABLE = 'FacebookAdsetEntity'
FB_CAMPAIGN_ENTITY_TABLE = 'FacebookCampaignEntity'
FB_ENTITY_REPORT_TYPE_TABLE = 'EntityReport'
FB_SWEEP_ENTITY_REPORT_TYPE_TABLE = 'SweepEntityReport'
FB_TOKEN_TABLE = 'FacebookPlatformToken'
JOB_REPORT_ENTITY_EXPECTATION_AD_ACCOUNT_INDEX_TABLE = 'JobReportEntityExpectationAdAccountIndex'
JOB_REPORT_ENTITY_EXPECTATION_TABLE = 'JobReportEntityExpectation'
JOB_REPORT_ENTITY_OUTCOME_TABLE = 'JobReportEntityOutcome'
JOB_REPORT_FAILURE_TABLE = 'JobReportFailure'


from common.updatefromenv import update_from_env
update_from_env(__name__)
