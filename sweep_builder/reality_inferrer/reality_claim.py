from datetime import datetime
from typing import Optional, Set

from common.enums.entity import Entity


class RealityClaim:
    """
    Used for passing a bundle of data from inner generator to upper one
    without having to decompose the bundle into individual parts

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """

    # Used to mark the scope from which the reality is to be infered
    # 'Console' for the data updated from the console api
    # None otherwise
    scope = None  # type: str

    ad_account_id = None  # type: str
    # entity_id may be same value as ad_account_id for AdAccount types (for consistency of interface)
    entity_id = None  # type: str
    entity_type = None  # type: str
    tokens = None  # type: Set[str]

    # Comes from parent AdAccount record
    timezone = None  # type: str

    # Campaign, AdSet, Ad objects may have these set
    # Beginning of Life Datetime
    bol = None  # type: datetime
    # End of Life Datetime
    eol = None  # type: Optional[datetime]
    # Hash of field values sorted and seen last time
    hash = None  # type: str
    # hash of LIST of fields asked for last time
    # (used to detect stale hashes.)
    hash_fields = None  # type: str

    def __init__(self, _data=None, **more_data):
        self.update(_data, **more_data)
        assert self.entity_id or self.scope # if entity id is not set check if scope is set
        assert self.entity_type in Entity.ALL

    def update(self, _data=None, **more_data):
        """
        Similar to native python's dict.update()

        :param _data: Positionally-provided Dict with data to apply to model
        :type _data: Optional[dict]
        :param more_data:
        :type more_data: dict
        :return:
        """
        if _data is not None:
            self.__dict__.update(**_data)
        self.__dict__.update(**more_data)

    def __repr__(self):
        return "<{} {} {}>".format(
            self.__class__.__name__,
            self.entity_type,
            self.entity_id
        )

    def to_dict(self):
        return self.__dict__.copy()
