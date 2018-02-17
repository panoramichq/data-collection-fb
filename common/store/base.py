from typing import Optional

from pynamodb.models import Model
from pynamodb import attributes
from pynamodb.expressions.update import Action as PynamoDBAction


from config import dynamodb as dynamodb_config


class BaseMeta:
    """
    Base Meta class used for configuring PynamoDB models
    Used to centralize setting of defaults
    """

    host = dynamodb_config.HOST

    table_name = None

    read_capacity_units = 1
    write_capacity_units = 1

    def __init__(self, table_name, **kwargs):
        self.__dict__.update(kwargs)
        self.table_name = table_name


def _convert_to_pynamodb_expression(cls, value, attr_name):
    if isinstance(value, PynamoDBAction):
        # it's already provided to us as compiled Operand
        # https://pynamodb.readthedocs.io/en/latest/updates.html#update-expressions
        return value

    if value is None:
        # instead of writing Null to DB, removing the value
        # This fixes a number of attribute types (like UTCDateTime)
        # that don't process well an actual value of None coming back from DB
        return getattr(cls, attr_name).remove()

    return getattr(cls, attr_name).set(value)


class BaseModel(Model):
    """
    Base PynamoDB models is cool, but needs more coolness. This is us.
    """

    @classmethod
    def upsert(cls, *primary_keys, **data):
        """
        Makes blind upsert operations a bit easier

        In DynamoDB model updates can be done with rather complex expressions
        https://pynamodb.readthedocs.io/en/latest/tutorial.html#updating-items

        This method allows idempotent upserts for record + selected data
        by communicating the data in simple dict format of attr_name: value.

        Default operation for each attribute is SET

        :param primary_keys:
        :param data:
        :return: Some DynamoDB update query goobledy-gook. Borderline useless to us.
        """
        # TODO: Implement Batch support https://pynamodb.readthedocs.io/en/latest/batch.html#batch-writes
        # (in prod, we will rarely use it, but it's useful in testing)

        model = cls(*primary_keys)

        actions = [
            _convert_to_pynamodb_expression(cls, value, attr_name)
            for attr_name, value in data.items()
        ]

        model.update(actions=actions)
        return model

    # allows to_dict method to learn about additional attributes / fields
    # on the model that we consider canonical, but these are not actually
    # stored in the database. This typically include derived data exposed
    # on the model as getter / property or static attributes like type enums
    # Contents of this set add to the self._attributes.keys() set of
    # attribute names specific to the model.
    _additional_fields = None  # type: Optional[set]

    # If set, this becomes the full default list of attributes
    # we export from the model in .to_dict call.
    # If set, this overrides (makes useless) value of _additional_fields
    _fields = None  # type: Optional[set]

    def to_dict(self, fields=None, skip_null=False):
        """
        Converts model into underlying data
        (but in human-readable attribute names, not the native / short DB-side names)

        If fields attribute

        :param fields: (Optional) list of attributes to extract.
            If None, default attributes are extracted.
        :type fields: list or set
        :rtype: dict
        """
        if not fields:
            fields = self._fields or \
                set(self._attributes.keys()) | (self._additional_fields or set())

        # TODO: this is inefficient (getattr twice) when skip_null=True. Rethink
        return {
            field: getattr(self, field)
            for field in fields
            if not skip_null or getattr(self, field) is not None
        }

    @classmethod
    def from_dict(cls, data):
        """
        It's here just for symmetry of API
        :param dict data:
        :rtype: cls
        """
        return cls(**data)
