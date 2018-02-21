import logging

from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from oozer.common.job_scope import JobScope

from .facebook_entity_collector import collect_entities_for_adaccount


app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
def fb_entities_adaccount_campaigns(job_scope, context):
    """
    Collect all campaign entities for a given adaccount

    :param JobScope job_scope: The dict representation of JobScope
    :param dict context:
    """
    cnt = 0
    for fb_model in collect_entities_for_adaccount(
        Entity.Campaign, job_scope, context
    ):
        cnt += 1
    logger.warning(f"fb_entities_adaccount_campaigns {job_scope.ad_account_id}:{cnt}")


@app.task
def fb_entities_adaccount_adsets(job_scope, context):
    """
    Collect all campaign entities for a given adaccount

    :param JobScope job_scope: The dict representation of JobScope
    :param dict context:
    """
    cnt = 0
    for fb_model in collect_entities_for_adaccount(
        Entity.AdSet, job_scope, context
    ):
        cnt += 1
    logger.warning(f"fb_entities_adaccount_adset {job_scope.ad_account_id}:{cnt}")


@app.task
def fb_entities_adaccount_ads(job_scope, context):
    """
    Collect all campaign entities for a given adaccount

    :param JobScope job_scope: The dict representation of JobScope
    :param dict context:
    """
    cnt = 0
    for fb_model in collect_entities_for_adaccount(
        Entity.Ad, job_scope, context
    ):
        cnt += 1
    logger.warning(f"fb_entities_adaccount_ad {job_scope.ad_account_id}:{cnt}")
