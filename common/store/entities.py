from common.twitter.enums.entity import Entity
from common.memoize import memoized_property
from config import dynamodb as dynamodb_config

from .base import BaseMeta, BaseModel, attributes


class TwitterAdAccountEntity(BaseModel):
    """
    Represents a single facebook ad account entity
    """
    Meta = BaseMeta(dynamodb_config.TW_AD_ACCOUNT_ENTITY_TABLE)

    _additional_fields = {
        'entity_type'
    }

    # scope is an ephemeral scoping element
    # Imagine "operam business manager system user" being one of the scope's values.
    # This is here mostly just to simplify iterating
    # through AAs per given known source.
    # (even if originally we will have only one scope in entire system)
    # At first, scope will be pegged one-to-one to
    # one token to be used for all AAs,
    # (Later this relationship may need to be inverted to
    # allow multiple tokens per AA)
    scope = attributes.UnicodeAttribute(hash_key=True, attr_name='scope')

    account_id = attributes.UnicodeAttribute(range_key=True, attr_name='aaid')

    # copied indicator of activity from Console DB per each sync
    # (alternative to deletion. To be discussed later if deletion is better)
    is_active = attributes.BooleanAttribute(default=False, attr_name='a')
    # utilized by logic that prunes out Ad Accounts
    # that are switched to "inactive" on Console
    # Expectation is that after a long-running update job
    # there is a task at the end that goes back and marks
    # all AA records with non-last-sweep_id as "inactive"
    # See https://operam.atlassian.net/browse/PROD-1825 for context
    updated_by_sweep_id = attributes.UnicodeAttribute(null=True, attr_name='u')

    # Each AdAccount on FB side can be set to a particular timezone
    # A lot of reporting on FB is pegged to a "day" that is interpreted
    # in that AdAccount's timezone (not UTC).
    timezone = attributes.UnicodeAttribute(attr_name='tz')

    entity_type = Entity.Account

    @property
    @memoized_property
    def scope_model(self):
        from .scope import AssetScope
        return AssetScope.get(self.scope)

    def to_tw_sdk_ad_account(self, client=None):
        """
        Returns an instance of Facebook Ads SDK AdAccount model
        with ID matching this DB model's ID

        :param twitter_ads.client.Client client: Twitter API client that is already set up
        :rtype: twitter_ads.
        """
        from twitter_ads.client import Client, Account

        if not client:
            # This is very expensive call. Takes 2 DB hits to get token value
            # Try to pass api value at all times in prod code to avoid using this.
            # Allowing to omit the API value to simplify use of this API in
            # testing in console.
            platform_token = self.scope_model.platform_token
            client = Client(
                consumer_key=platform_token.consumer_key,
                consumer_secret= platform_token.consumer_secret,
                access_token=platform_token.token,
                access_token_secret=platform_token.secret
            )

        return Account(client=client)


class TwitterEntityBaseMixin:
    """
    Use this mixin for describing Facebook entity existence tables
    """

    # Primary Keys

    # Hash Key (old name) == Primary Key (new name)
    # Range Key (old name) == Sort Key (new name) [ == Secondary Key (Cassandra term, used by Daniel D) ]
    # See https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/

    # Note that each Entity is keyed by, effectively, a compound key: ad_account_id+entity_id
    # This allows us to issue queries like "Get all objects per ad_account_id" rather quickly
    account_id = attributes.UnicodeAttribute(hash_key=True, attr_name='aid')
    # do NOT set an index on secondary keys (unless you really really need it)
    # In DynamoDB this limits the table size to 10GB
    # Without secondary key index, table size is unbounded.
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.ItemCollections.SizeLimit
    entity_id = attributes.UnicodeAttribute(range_key=True, attr_name='eid')

    bol = attributes.UTCDateTimeAttribute(null=True)
    eol = attributes.UTCDateTimeAttribute(null=True)
    hash = attributes.UnicodeAttribute(null=True, attr_name='h')  # Could be binary
    hash_fields = attributes.UnicodeAttribute(null=True, attr_name='hf')  # Could be binary

    entity_type = None  # will be overridden in subclass
    _additional_fields = {
        'entity_type'
    }


class EntityBaseMeta(BaseMeta):
    # Entity tables will be written to massively in parallel
    # but are read by single thread.
    read_capacity_units = 2
    write_capacity_units = 10


class TwitterCampaignEntity(TwitterEntityBaseMixin, BaseModel):
    """
    Represents a single facebook campaign entity
    """
    Meta = EntityBaseMeta(dynamodb_config.TW_CAMPAIGN_ENTITY_TABLE)

    entity_type = Entity.Campaign


class TwitterLineItemEntity(TwitterEntityBaseMixin, BaseModel):
    """
    Represent a single facebook adset entity
    """
    Meta = EntityBaseMeta(dynamodb_config.TW_ADSET_ENTITY_TABLE)

    entity_type = Entity.LineItem


class TwitterPromotedTweetEntity(TwitterEntityBaseMixin, BaseModel):
    """
    Represents a single facebook ad entity
    """
    Meta = EntityBaseMeta(dynamodb_config.TW_AD_ENTITY_TABLE)

    entity_type = Entity.PromotedTweet


# Used to map from entity_type str to Model for persistence-style tasks
ENTITY_TYPE_MODEL_MAP = {
    model.entity_type: model
    for model in [
        TwitterAdAccountEntity,
        TwitterCampaignEntity,
        TwitterLineItemEntity,
        TwitterPromotedTweetEntity
    ]
}


def sync_schema(brute_force=False):
    """
    In order to push fidelity and maintenance of table "migrations"
    closer to the code where the models are migrated, this is where
    we'll hook up generically-reused API for upserting our tables.
    Call this from some centralized `sync_schema` script
    """
    from pynamodb.exceptions import TableError, TableDoesNotExist

    tables = [
        TwitterAdAccountEntity,
        TwitterCampaignEntity,
        TwitterLineItemEntity,
        TwitterPromotedTweetEntity
    ]

    for table in tables:
        # create_table does NOTHING if table already exists - bad
        # when we add keys in the model, nothing will happen below.
        # TODO: adapt this to update as well
        # until then, every time we need to update the tables, just delete
        # them all.
        if brute_force:
            try:
                table.delete_table()
            except (TableError, TableDoesNotExist):
                pass
        table.create_table()
