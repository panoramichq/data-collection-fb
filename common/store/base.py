from pynamodb.models import Model
from pynamodb import attributes

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
            getattr(cls, attr_name).set(value)
            for attr_name, value in data.items()
        ]
        model.update(actions=actions)
        return model

    def to_dict(self, fields=None, skip_null=False):
        """
        Converts model into underlying data
        (but in human-readable attribute names, not the

        :param list fields: list of attributes to extract. If None, all attributes.
        :rtype: dict
        """
        return {
            attr_name: getattr(self, attr_name)
            for attr_name in self._attributes.keys()
            # return if fields in not set, or if set, attr name must be in fields list
            if (not fields or attr_name in fields)
        }

    @classmethod
    def from_dict(cls, data):
        """
        It's here just for symmetry of API
        :param dict data:
        :rtype: cls
        """
        return cls(**data)
