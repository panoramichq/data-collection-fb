from datetime import datetime

from common.celeryapp import get_celery_app
from common.entity_types import get_entity_model_from_entity_name

app = get_celery_app()


@app.task
def feedback_entity(entity_type, entity, entity_hash):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param string entity_type: Type of the entity, a string representation
    :param dict entity: The entity we're feeding back to the system
    :param tuple(string, string) entity_hash: Tuple containing both entity data
        itself and fields hashes that we can use

    """
    klazz = get_entity_model_from_entity_name(entity_type)

    entity_id, ad_account_id = entity['id'], entity['account_id']

    def parse_fb_datetime(value):
        """
        Helper to parse facebooks datetime string
        :param string value: The original datetime value

        :return datetime|None: Parsed datetime string
        """
        if not value:
            return

        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S%z')

    # Figure out BOL and EOL
    bol = parse_fb_datetime(entity.get('created_time'))
    eol = parse_fb_datetime(entity.get('updated_time')) \
        if entity.get('effective_status') in ['ARCHIVED', 'DELETED'] else None

    # Can't use upsert, pynamo chokes on null attributes with `update`
    klazz(
        ad_account_id=ad_account_id,
        entity_id=entity_id,
        bol=bol,
        eol=eol,
        hash=entity_hash[0],
        hash_fields=entity_hash[1],
    ).save()
