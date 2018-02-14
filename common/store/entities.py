from config import dynamodb as dynamodb_config

from .base import BaseMeta, BaseModel, attributes


class FacebookEntityBaseMixin:
    """
    Use this mixin for describing Facebook entity existence tables
    """

    # Primary Keys

    # Hash Key (old name) == Primary Key (new name)
    # Range Key (old name) == Sort Key (new name) [ == Secondary Key (Cassandra term, used by Daniel D) ]
    # See https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/

    # Note that each Entity is keyed by, effectively, a compound key: ad_account_id+entity_id
    # This allows us to issue queries like "Get all objects per ad_account_id" rather quickly
    ad_account_id = attributes.UnicodeAttribute(hash_key=True, attr_name='aaid')
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


class EntityBaseMeta(BaseMeta):
    # Entity tables will be written to massively in parallel
    # but are read by single thread.
    read_capacity_units = 2
    write_capacity_units = 10


class FacebookCampaignEntity(FacebookEntityBaseMixin, BaseModel):
    """
    Represents a single facebook campaign entity
    """
    Meta = EntityBaseMeta(dynamodb_config.FB_CAMPAIGN_ENTITY_TABLE)


class FacebookAdsetEntity(FacebookEntityBaseMixin, BaseModel):
    """
    Represent a single facebook adset entity
    """
    Meta = EntityBaseMeta(dynamodb_config.FB_ADSET_ENTITY_TABLE)


class FacebookAdEntity(FacebookEntityBaseMixin, BaseModel):
    """
    Represents a single facebook ad entity
    """
    Meta = EntityBaseMeta(dynamodb_config.FB_AD_ENTITY_TABLE)


def sync_schema(brute_force=False):
    """
    In order to push fidelity and maintenance of table "migrations"
    closer to the code where the models are migrated, this is where
    we'll hook up generically-reused API for upserting our tables.
    Call this from some centralized `sync_schema` script
    """
    from pynamodb.exceptions import TableError, TableDoesNotExist

    tables = [
        FacebookCampaignEntity,
        FacebookAdsetEntity,
        FacebookAdEntity
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
