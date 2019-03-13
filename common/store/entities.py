from typing import Any

from common.enums.entity import Entity
from common.memoize import memoized_property
from config import dynamodb as dynamodb_config
from oozer.common.job_scope import JobScope

from .base import BaseMeta, BaseModel, attributes


class ConsoleEntityMixin:
    @classmethod
    def upsert_entity_from_console(cls, job_scope: JobScope, entity: Any):
        pass


class AdAccountEntity(ConsoleEntityMixin, BaseModel):
    """
    Represents a single facebook ad account entity
    """
    Meta = BaseMeta(dynamodb_config.AD_ACCOUNT_ENTITY_TABLE)

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

    ad_account_id = attributes.UnicodeAttribute(range_key=True, attr_name='aaid')

    # copied indicator of activity from Console DB per each sync
    # (alternative to deletion. To be discussed later if deletion is better)
    is_active = attributes.BooleanAttribute(default=False, attr_name='a')

    # Provides an option to manually disable accounts from syncing, even if they are imported as active from console.
    manually_disabled = attributes.BooleanAttribute(default=False, attr_name='man_dis')

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

    entity_type = Entity.AdAccount

    @property
    @memoized_property
    def scope_model(self):
        from .scope import AssetScope
        return AssetScope.get(self.scope)

    def to_fb_sdk_ad_account(self, api=None):
        """
        Returns an instance of Facebook Ads SDK AdAccount model
        with ID matching this DB model's ID

        :param facebook_business.api.FacebookAdsApi api: FB Ads SDK Api instance with token baked in.
        :rtype: facebook_business.adobjects.adaccount.AdAccount
        """
        from facebook_business.api import FacebookAdsApi, FacebookSession
        from facebook_business.adobjects.adaccount import AdAccount

        if not api:
            # This is very expensive call. Takes 2 DB hits to get token value
            # Try to pass api value at all times in prod code to avoid using this.
            # Allowing to omit the API value to simplify use of this API in
            # testing in console.
            api = FacebookAdsApi(FacebookSession(access_token=self.scope_model.platform_token))

        return AdAccount(fbid=f'act_{self.ad_account_id}', api=api)

    @classmethod
    def upsert_entity_from_console(cls, job_scope: JobScope, entity: Any):
        cls.upsert(
            job_scope.entity_id,  # scope ID
            entity['ad_account_id'],
            is_active=entity.get('active', True),
            updated_by_sweep_id=job_scope.sweep_id
        )


class EntityBaseMixin:
    """
    Use this mixin for describing Facebook entity existence tables
    """

    # Primary Keys

    # Hash Key (old name) == Primary Key (new name)
    # Range Key (old name) == Sort Key (new name) [ == Secondary Key (Cassandra term, used by Daniel D) ]
    # See https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/

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

    # Since we use UpdateItem for inserting records, we must have at least
    # one attribute specified on each model. Normally that would be created_time
    # but FB doesn't have that on all models, so we setup a default
    _default_bol = False

    entity_type = None  # will be overridden in subclass
    _additional_fields = {
        'entity_type'
    }


class AdEntityBaseMixin(EntityBaseMixin):
    # Note that each Entity is keyed by, effectively, a compound key: ad_account_id+entity_id
    # This allows us to issue queries like "Get all objects per ad_account_id" rather quickly
    ad_account_id = attributes.UnicodeAttribute(hash_key=True, attr_name='aaid')


class PageEntityBaseMixin:
    page_id = attributes.UnicodeAttribute(hash_key=True, attr_name='pid')

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


class CampaignEntity(AdEntityBaseMixin, BaseModel):
    """
    Represents a single facebook campaign entity
    """
    Meta = EntityBaseMeta(dynamodb_config.CAMPAIGN_ENTITY_TABLE)

    entity_type = Entity.Campaign


class AdsetEntity(AdEntityBaseMixin, BaseModel):
    """
    Represent a single facebook adset entity
    """
    Meta = EntityBaseMeta(dynamodb_config.ADSET_ENTITY_TABLE)

    entity_type = Entity.AdSet


class AdEntity(AdEntityBaseMixin, BaseModel):
    """
    Represents a single facebook ad entity
    """
    Meta = EntityBaseMeta(dynamodb_config.AD_ENTITY_TABLE)

    entity_type = Entity.Ad


class AdCreativeEntity(AdEntityBaseMixin, BaseModel):
    """
    Represents a single facebook ad creative entity
    """

    Meta = EntityBaseMeta(dynamodb_config.AD_CREATIVE_ENTITY_TABLE)

    entity_type = Entity.AdCreative
    _default_bol = True


class AdVideoEntity(AdEntityBaseMixin, BaseModel):
    """
    Represents a single facebook ad creative entity
    """

    Meta = EntityBaseMeta(dynamodb_config.AD_VIDEO_ENTITY_TABLE)

    entity_type = Entity.AdVideo
    _default_bol = True


class CustomAudienceEntity(AdEntityBaseMixin, BaseModel):
    """
    Represents a single facebook ad creative entity
    """

    Meta = EntityBaseMeta(dynamodb_config.CUSTOM_AUDIENCE_ENTITY_TABLE)

    entity_type = Entity.CustomAudience


class PageEntity(ConsoleEntityMixin, BaseModel):
    """
    Represents a single facebook page entity
    """

    Meta = EntityBaseMeta(dynamodb_config.PAGE_ENTITY_TABLE)

    scope = attributes.UnicodeAttribute(hash_key=True, attr_name='scope')
    page_id = attributes.UnicodeAttribute(range_key=True, attr_name='pid')

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

    entity_type = Entity.Page

    _additional_fields = {
        'entity_type'
    }
    _default_bol = True

    @classmethod
    def upsert_entity_from_console(cls, job_scope: JobScope, entity: Any):
        cls.upsert(
            job_scope.entity_id,  # scope ID
            entity['ad_account_id'],
            is_active=entity.get('active', True),
            updated_by_sweep_id=job_scope.sweep_id
        )


class PagePostEntity(PageEntityBaseMixin, BaseModel):
    """
    Represents a single facebook page post entity
    """

    Meta = EntityBaseMeta(dynamodb_config.PAGE_POST_ENTITY_TABLE)
    entity_id = attributes.UnicodeAttribute(range_key=True, attr_name='eid')

    entity_type = Entity.PagePost
    _default_bol = True


# Used to map from entity_type str to Model for persistence-style tasks
ENTITY_TYPE_MODEL_MAP = {
    model.entity_type: model
    for model in [
        AdAccountEntity,
        CampaignEntity,
        AdsetEntity,
        AdEntity,
        AdCreativeEntity,
        AdVideoEntity,
        CustomAudienceEntity,
        PageEntity,
        PagePostEntity,
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
        AdAccountEntity,
        CampaignEntity,
        AdsetEntity,
        AdEntity,
        AdCreativeEntity,
        AdVideoEntity,
        CustomAudienceEntity,
        PageEntity,
        PagePostEntity,
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
