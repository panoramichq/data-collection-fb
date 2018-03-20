# can be overridden by explicit DynamoDB server host URL
# that PynamoDB models accept as `host` attribute
# leaving this a None makes PynamoDB fall back to
# region settings
HOST = None

TW_AD_ACCOUNT_SCOPE_TABLE = 'TwitterAdAccountScope'
TW_AD_ACCOUNT_ENTITY_TABLE = 'TwitterAdAccountEntity'
TW_TOKEN_TABLE = 'TwitterPlatformToken'

TW_CAMPAIGN_ENTITY_TABLE = 'TwitterCampaignEntity'
TW_LINE_ITEM_ENTITY_TABLE = 'TwitterLineItemEntity'
TW_PROMOTED_TWEET_ENTITY_TABLE = 'TwitterPromotedTweetEntity'

TW_ENTITY_REPORT_TYPE_TABLE = 'EntityReport'
TW_SWEEP_ENTITY_REPORT_TYPE_TABLE = 'SweepEntityReport'

from common.updatefromenv import update_from_env
update_from_env(__name__)
