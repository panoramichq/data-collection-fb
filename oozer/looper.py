from typing import Generator, Callable

from common.connect.redis import get_redis
from common.enums.reporttype import ReportType
from common.enums.entity import Entity
from common.id_tools import parse_id
from oozer.common.job_scope import JobScope


# :) Guess what for
FIRST = 0
LAST = -1


def iter_tasks_from_redis_zkey(zkey):
    redis = get_redis()
    start = 0
    step = 200

    job_ids = redis.zrevrange(zkey, start, start+step)

    while job_ids:
        for job_id in job_ids:
            yield job_id.decode('utf8')

        start += step
        job_ids = redis.zrevrange(zkey, start, start+step)


def get_tasks_map():
    # inside of function call to avoid circular import errors

    from oozer.entities.tasks import (
        fb_entities_adaccount_campaigns,
        fb_entities_adaccount_adsets,
        fb_entities_adaccount_ads
    )

    from oozer.metrics.tasks import (
        fb_insights_adaccount_campaigns_lifetime
    )

    return {
        ReportType.entities: {
            Entity.Campaign: fb_entities_adaccount_campaigns,
            Entity.AdSet: fb_entities_adaccount_adsets,
            Entity.Ad: fb_entities_adaccount_ads,
        },
        ReportType.lifetime: {
            Entity.Campaign: fb_insights_adaccount_campaigns_lifetime
        }
    }


def iter_tasks(sweep_id):
    """
    Persist prioritized jobs and pass-through context objects for inspection

    :param str sweep_id:
    :rtype: Generator[PrioritizationClaim]
    """
    from config.facebook import TOKEN

    inventory = get_tasks_map()

    for job_id in iter_tasks_from_redis_zkey(sweep_id):

        parts = parse_id(job_id)  # type: dict

        report_type = parts['report_type']
        entity_type = parts['entity_type'] or parts['report_variant']

        celery_task = inventory.get(report_type, {}).get(entity_type)

        if celery_task:
            job_scope = JobScope(
                parts,
                tokens=[TOKEN]
            )

            celery_task.delay(job_scope, {})

        yield job_id
