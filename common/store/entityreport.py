from config import dynamodb as dynamodb_config

from .base import BaseMeta, BaseModel, attributes


class TwitterEntityReport(BaseModel):
    """
    Use this mixin for describing Facebook entity existence tables
    """
    Meta = BaseMeta(
        dynamodb_config.TW_ENTITY_REPORT_TYPE_TABLE,
        read_capacity_units=5,
        write_capacity_units=20
    )
    # Primary Keys
    # Hash Key (old name) == Primary Key (new name)
    # Range Key (old name) == Sort Key (new name) [ == Secondary Key (Cassandra term, used by Daniel D) ]
    # See https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/

    # Note that each record is keyed by, effectively, a compound key: entity_report_type+coverage_period
    # This allows us to issue queries like "Get all objects per entity_report_type" rather quickly
    # this is some compound ID, encompasing entity_compound_id and task type enum
    entity_report_type = attributes.UnicodeAttribute(hash_key=True, attr_name='ert')

    # do NOT set an index on secondary keys (unless you really really need it)
    # In DynamoDB this limits the table size to 10GB
    # Without secondary key index, table size is unbounded.
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.ItemCollections.SizeLimit
    coverage_period = attributes.UnicodeAttribute(range_key=True, attr_name='dt')

    last_attempt = attributes.UTCDateTimeAttribute(null=True, attr_name='a')
    last_stage_id = attributes.NumberAttribute(null=True, attr_name='stid')
    last_success = attributes.UTCDateTimeAttribute(null=True, attr_name='s')
    last_success_build_id = attributes.UnicodeAttribute(null=True, attr_name='sbid')
    last_failure = attributes.UTCDateTimeAttribute(null=True, attr_name='f')
    last_failure_bucket = attributes.NumberAttribute(null=True, attr_name='fb')


def sync_schema(brute_force=False):
    """
    In order to push fidelity and maintenance of table "migrations"
    closer to the code where the models are migrated, this is where
    we'll hook up generically-reused API for upserting our tables.
    Call this from some centralized `sync_schema` script
    """
    from pynamodb.exceptions import TableError, TableDoesNotExist

    tables = [
        TwitterEntityReport
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
