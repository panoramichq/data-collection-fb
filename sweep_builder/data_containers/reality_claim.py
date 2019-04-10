from datetime import datetime
from typing import Set

from common.enums.entity import Entity


class RealityClaim:
    """
    Used for passing a bundle of data from inner generator to upper one
    without having to decompose the bundle into individual parts

    Think of it as "context" object - a dumping ground of data pertinent to entity's existence

    (Used to avoid the need to change all functions in the stack if you need
    to add more data to context from the very bottom of the stack. Just extend this object.)
    """

    # entity_id may be same value as ad_account_id for AdAccount types (for consistency of interface)
    # entity_id value may actually be scope ID for claims of Scope's existence

    entity_id: str = None
    entity_type: str = None

    ad_account_id: str = None

    tokens: Set[str] = None

    # Comes from parent AdAccount record
    timezone: str = None

    # Campaign, AdSet, Ad objects may have these set
    # Beginning of Life Datetime
    bol: datetime = None
    # End of Life Datetime
    eol: datetime = None

    def __init__(self, _data=None, **more_data):
        self.update(_data, **more_data)
        assert self.entity_id
        assert self.entity_type in Entity.ALL

    def update(self, _data=None, **more_data):
        """
        Similar to native python's dict.update()
        """
        if _data is not None:
            self.__dict__.update(**_data)
        self.__dict__.update(**more_data)

    def __repr__(self) -> str:
        return "<{0} {1} {2}>".format(self.__class__.__name__, self.entity_type, self.entity_id)

    def to_dict(self):
        return self.__dict__.copy()
