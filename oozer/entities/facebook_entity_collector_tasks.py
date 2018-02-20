from common.celeryapp import get_celery_app
from common.enums.entity import Entity

from .facebook_entity_collector import collect_entities_for_adaccount


app = get_celery_app()



@app.task
def fb_entities_adaccount_campaigns(job_scope, context):
    """
    Collect all campaign entities for a given adaccount

    :param dict job_scope: The dict representation of JobScope
    :param dict context:
    """
    # TODO: possibly rehydrate JobScope for easier manipulation

    collect_entities_for_adaccount(
        Entity.Campaign, job_scope, context
    )


@app.task
def fb_entities_adaccount_adsets(job_scope, context):
    """
    Collect all campaign entities for a given adaccount

    :param dict job_scope: The dict representation of JobScope
    :param dict context:
    """
    # TODO: possibly rehydrate JobScope for easier manipulation

    collect_entities_for_adaccount(
        Entity.AdSet, job_scope, context
    )


@app.task
def fb_entities_adaccount_ads(job_scope, context):
    """
    Collect all campaign entities for a given adaccount

    :param dict job_scope: The dict representation of JobScope
    :param dict context:
    """
    # TODO: possibly rehydrate JobScope for easier manipulation

    collect_entities_for_adaccount(
        Entity.Ad, job_scope, context
    )
