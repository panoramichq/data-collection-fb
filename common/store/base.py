from typing import List, Set, Any, Dict

from queue import deque
from pynamodb.models import Model, BatchWrite as _BatchWrite

from config import dynamodb as dynamodb_config


class BaseMeta:
    """
    Base Meta class used for configuring PynamoDB models
    Used to centralize setting of defaults
    """

    host: str = dynamodb_config.HOST

    table_name: str = None

    read_capacity_units: int = 1
    write_capacity_units: int = 1

    def __init__(self, table_name, **kwargs):
        self.__dict__.update(kwargs)
        self.table_name: str = table_name


def _convert_to_pynamodb_expression(cls, value, attr_name):
    # https://pynamodb.readthedocs.io/en/latest/updates.html#update-expressions
    if value is None:
        # instead of writing Null to DB, removing the value
        # This fixes a number of attribute types (like UTCDateTime)
        # that don't process well an actual value of None coming back from DB
        return getattr(cls, attr_name).remove()

    return getattr(cls, attr_name).set(value)


class BatchWrite(_BatchWrite):
    def __init__(self, model, auto_commit=True, key_cache_size=500):
        super().__init__(model, auto_commit=auto_commit)
        self._key_cache: Set[str] = set()
        self._key_cache_order = deque()
        self._key_cache_remaining: int = key_cache_size

    def upsert_deduped(self, *primary_keys, **data):
        """
        A variant of upsert() that tries not to write too much
        when it detects reuse of the primary key.

        Only first record is let out, but because
        the key cache is finite, same key may be considered "first"
        if it repeats outside of cache size recycle.
        """
        if primary_keys in self._key_cache:
            return

        self._key_cache.add(primary_keys)

        # we are popping off cache in FIFO order
        self._key_cache_order.append(primary_keys)  # on right side
        if self._key_cache_remaining is 0:
            item = self._key_cache_order.popleft()
            self._key_cache.remove(item)
        else:
            self._key_cache_remaining -= 1

        self.upsert(*primary_keys, **data)

    def upsert(self, *primary_keys, **data):
        """
        Convenience method that saves us from initing model by hand
        when using base BatchWrite.save(model_instance)

        Not really an update, since entire model is overwritten,
        but, luckily, quietly whether it exists or not.
        """
        self.save(self.model(*primary_keys, **data))


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
        """
        # TODO: Implement Batch support https://pynamodb.readthedocs.io/en/latest/batch.html#batch-writes
        # (in prod, we will rarely use it, but it's useful in testing)

        model = cls(*primary_keys)

        actions = [_convert_to_pynamodb_expression(cls, value, attr_name) for attr_name, value in data.items()]

        model.update(actions=actions)
        return model

    @classmethod
    def batch_write(cls, auto_commit: bool = True):
        """
        Returns a context manager for a batch operation'

        Overrides base method. Returns enhanced version of BatchWrite.

        :param auto_commit: Commits writes automatically if `True`
        """
        return BatchWrite(cls, auto_commit=auto_commit)

    # allows to_dict method to learn about additional attributes / fields
    # on the model that we consider canonical, but these are not actually
    # stored in the database. This typically include derived data exposed
    # on the model as getter / property or static attributes like type enums
    # Contents of this set add to the self._attributes.keys() set of
    # attribute names specific to the model.
    _additional_fields: Set[str] = None

    # If set, this becomes the full default list of attributes
    # we export from the model in .to_dict call.
    # If set, this overrides (makes useless) value of _additional_fields
    _fields: Set[str] = None

    def to_dict(self, fields: List[str] = None, skip_null: bool = False):
        """
        Converts model into underlying data
        (but in human-readable attribute names, not the native / short DB-side names)
        """
        if not fields:
            fields = self._fields or set(self._attributes.keys()) | (self._additional_fields or set())

        # TODO: this is inefficient (getattr twice) when skip_null=True. Rethink
        return {field: getattr(self, field) for field in fields if not skip_null or getattr(self, field) is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """
        It's here just for symmetry of API
        """
        return cls(**data)
