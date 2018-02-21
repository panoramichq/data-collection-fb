from datetime import datetime
from dateutil.parser import parse as parse_iso_datetime_string

from common.store.entities import ENTITY_TYPE_MODEL_MAP
from common.tztools import dt_to_other_timezone
from common.enums.entity import Entity


def _parse_fb_datetime(value):
    """
    Helper to parse facebooks datetime string
    :param string value: The original datetime value

    :return datetime|None: Parsed datetime string
    """
    if not value:
        return None

    if isinstance(value, datetime):
        return dt_to_other_timezone(
            value,
            'UTC'
        )

    return dt_to_other_timezone(
        parse_iso_datetime_string(value),
        'UTC'
    )


def feedback_entity(entity_data, entity_type, entity_hash_pair):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param dict entity_data: The entity we're feeding back to the system
    :param string entity_type: Type of the entity, a string representation
    :param tuple(string, string) entity_hash_pair: Tuple containing both entity data
        itself and fields hashes that we can use

    """

    if entity_type not in Entity.ALL:
        raise ValueError(f'Argument "entity_type" must be one of {Entity.ALL}. Received "{entity_type}" instead.')
    if not isinstance(entity_data, dict):
        raise ValueError(f'Argument "entity_data" must be an instance of Dict type. Received "{type(entity_data)}" instead.')

    Model = ENTITY_TYPE_MODEL_MAP[entity_type]

    entity_id = entity_data['id']
    ad_account_id = entity_data['account_id']

    bol = _parse_fb_datetime(entity_data.get('created_time'))

    # End of Life (for metrics purposes) occurs when Entity status
    # turns from whatever, to "irreversible death" - Archived or Deleted.
    # We guess that last update is a safe bet to treat as "it was turned off then" datetime
    # Thus speculatively deriving EOL from the last update if "irreversible death" is detected
    eol = _parse_fb_datetime(entity_data.get('updated_time')) \
        if (entity_data.get('configured_status') or entity_data.get('effective_status')) in ['ARCHIVED', 'DELETED'] \
        else None

    # Note on Model.attr | value use:
    # This is a way to express "set if does not exist" logic
    # https://pynamodb.readthedocs.io/en/latest/updates.html#update-expressions

    upsert_data = dict(
        hash=entity_hash_pair[0],
        hash_fields=entity_hash_pair[1],
    )

    # if bol:
    #     upsert_data['bol'] = Model.bol | bol  # here we allow computed / manual value to stand against "created_time"
    # if eol:
    #     upsert_data['eol'] = Model.eol | eol  # allow previously computed value to stand against new value
    # Model.upsert(
    #     ad_account_id,
    #     entity_id,
    #     **upsert_data
    # )

    # Cannot use Model.attr | value format above ^ because PynamoDB does not support full list of Operands
    # on Model.update(**d) call. Only Set, Add, Delete, Remove actions are allowed.
    # TODO: connect with developer, see if this is a real limitation (no support on DynamoDB side) or an accidental omission
    #       Possibly help them roll that feature in.

    # Until then, have to read the record in before updating it.
    try:
        record = Model.get(ad_account_id, entity_id)
    except Model.DoesNotExist:
        record = None

    if record:
        # We must take care NOT to override the values already set for these attrs
        if not record.bol and bol:
            upsert_data['bol'] = bol
        if not record.eol and eol:
            upsert_data['eol'] = eol

        for attr_name, value in upsert_data.items():
            setattr(record, attr_name, value)
        record.save()

    else:
        if bol:
            upsert_data['bol'] = bol
        if eol:
            upsert_data['eol'] = eol

        Model.upsert(
            ad_account_id,
            entity_id,
            **upsert_data
        )
