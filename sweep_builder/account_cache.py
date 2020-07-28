from datetime import datetime
from typing import Dict, Optional

from common.store.entities import AdAccountEntity


class AccountCache:

    scope = 'Console'
    _cache: Dict[str, AdAccountEntity] = {}

    @classmethod
    def get_model(cls, account_id):
        if account_id not in cls._cache:
            try:
                aa = AdAccountEntity.get(cls.scope, account_id)
            except AdAccountEntity.DoesNotExist:
                aa = None
            cls._cache[account_id] = aa
        return cls._cache[account_id]

    @classmethod
    def get_score_multiplier(cls, account_id) -> Optional[float]:
        m = cls.get_model(account_id)
        return m.score_multiplier if m else None

    @classmethod
    def get_refresh_if_older_than(cls, account_id) -> Optional[datetime]:
        m = cls.get_model(account_id)
        return m.refresh_if_older_than if m else None

    @classmethod
    def reset(cls):
        cls._cache.clear()
