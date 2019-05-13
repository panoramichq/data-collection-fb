from datetime import datetime
from typing import Dict, Any, Optional

from dateutil.parser import parse as parse_iso_datetime_string

from common.store.entities import ENTITY_TYPE_MODEL_MAP, AdAccountEntity
from common.store.scope import DEFAULT_SCOPE
from common.tztools import dt_to_other_timezone, now
from common.enums.entity import Entity

# this remapping is done so we can map promotable posts to ordinary posts in dynamo tables
REMAPPING_ENTITY_TYPE_FEEDBACK = {Entity.PagePostPromotable: Entity.PagePost}


def _parse_fb_datetime(value) -> Optional[datetime]:
    """
    Helper to parse facebooks datetime string
    :param value: The original datetime value

    :return Parsed datetime string
    """
    if not value:
        return None

    if isinstance(value, datetime):
        return dt_to_other_timezone(value, 'UTC')

    return dt_to_other_timezone(parse_iso_datetime_string(value), 'UTC')


_eol_status = {'ARCHIVED', 'DELETED'}


def feedback_entity(entity_data: Dict[str, Any], entity_type: str):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param entity_data: The entity we're feeding back to the system
    :param entity_type: Type of the entity, a string representation
    """
    if entity_type not in Entity.ALL:
        raise ValueError(f'Argument "entity_type" must be one of {Entity.ALL}. Received "{entity_type}" instead.')
    if not isinstance(entity_data, dict):
        raise ValueError(
            f'Argument "entity_data" must be an instance of Dict type. Received "{type(entity_data)}" instead.'
        )

    if entity_type == Entity.AdAccount:
        _upsert_ad_account_entity(entity_data, entity_type)
    else:
        _upsert_regular_entity(entity_data, entity_type)


def _upsert_ad_account_entity(entity_data: Dict[str, Any], entity_type: str):
    assert entity_type == Entity.AdAccount
    upsert_data = {'timezone': entity_data['timezone_name']}
    ad_account_id = entity_data['account_id']
    # The scope enum here must be hardcoded to Console (it is not available on JobScope or entity data).
    # Will have to be changed once we get more than one scope.
    AdAccountEntity.upsert(DEFAULT_SCOPE, ad_account_id, **upsert_data)


def determine_ad_account_id(entity_data: Dict[str, Any], entity_type: str) -> str:
    if entity_type == Entity.AdAccount:
        ad_account_id = entity_data['id']
    else:
        ad_account_id = entity_data['page_id'] if entity_type in Entity.NON_AA_SCOPED else entity_data['account_id']

    return ad_account_id


def _upsert_regular_entity(entity_data: Dict[str, Any], entity_type: str):
    if entity_type not in Entity.ALL:
        raise ValueError(f'Argument "entity_type" must be one of {Entity.ALL}. Received "{entity_type}" instead.')
    if not isinstance(entity_data, dict):
        raise ValueError(
            f'Argument "entity_data" must be an instance of Dict type. Received "{type(entity_data)}" instead.'
        )

    entity_type = REMAPPING_ENTITY_TYPE_FEEDBACK.get(entity_type, entity_type)
    Model = ENTITY_TYPE_MODEL_MAP[entity_type]

    entity_id = entity_data['id']
    ad_account_id = determine_ad_account_id(entity_data, entity_type)

    # Custom audiences specify create & update time as unix timestamps
    if entity_data.get('time_created'):
        entity_data['created_time'] = datetime.fromtimestamp(entity_data['time_created'])
    if entity_data.get('time_updated'):
        entity_data['updated_time'] = datetime.fromtimestamp(entity_data['time_updated'])

    # Handle default BOL if the value doesn't exist in the data
    if Model._default_bol and not entity_data.get('created_time'):
        entity_data['created_time'] = now()

    bol = _parse_fb_datetime(entity_data.get('created_time'))

    # End of Life (for metrics purposes) occurs when Entity status
    # turns from whatever, to "irreversible death" - Archived or Deleted.
    # We guess that last update is a safe bet to treat as "it was turned off then" datetime
    # Thus speculatively deriving EOL from the last update if "irreversible death" is detected
    # We cast this to a string to avoid any issues with entity types using "status" as a dict
    _is_eol = (
        str(entity_data.get('configured_status') or entity_data.get('effective_status') or entity_data.get('status'))
        in _eol_status
    )

    eol = _parse_fb_datetime(entity_data.get('updated_time')) if _is_eol else None

    upsert_data = {'is_accessible': None}

    campaign_id = entity_data.get('campaign_id')
    if campaign_id is not None:
        upsert_data['campaign_id'] = campaign_id

    adset_id = entity_data.get('adset_id')
    if adset_id is not None:
        upsert_data['adset_id'] = adset_id

    # Note on Model.attr | value use:
    # This is a way to express "set if does not exist" logic
    # https://pynamodb.readthedocs.io/en/latest/updates.html#update-expressions

    if bol:
        upsert_data['bol'] = Model.bol | bol  # here we allow computed / manual value to stand against "created_time"
    if eol:
        upsert_data['eol'] = Model.eol | eol  # allow previously computed value to stand against new value

    if upsert_data:
        Model.upsert(ad_account_id, entity_id, **upsert_data)
