from typing import Generator, Tuple

from common.connect.redis import get_redis
from common.enums.reporttype import ReportType
from common.enums.entity import Entity
from common.id_tools import parse_id
from oozer.common.job_scope import JobScope
from oozer.common.job_context import JobContext


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

    from oozer.entities.tasks import collect_entities_per_adaccount_task
    from oozer.metrics.tasks import collect_insights_task

    # handlers are often the same for each type because they
    # look at JobScope to figure out the particular data collection mode
    return {
        ReportType.entities: {
            Entity.Campaign: collect_entities_per_adaccount_task,
            Entity.AdSet: collect_entities_per_adaccount_task,
            Entity.Ad: collect_entities_per_adaccount_task,
        },
        ReportType.lifetime: {
            Entity.Campaign: collect_insights_task,
            Entity.AdSet: collect_insights_task,
            Entity.Ad: collect_insights_task,
        },
        ReportType.day_age_gender: {
            Entity.Ad: collect_insights_task
        },
        ReportType.day_dma: {
            Entity.Ad: collect_insights_task
        },
        ReportType.day_hour: {
            Entity.Ad: collect_insights_task
        }
    }


def iter_tasks(sweep_id):
    """
    Persist prioritized jobs and pass-through context objects for inspection

    :param str sweep_id:
    :rtype: Generator[Tuple[CeleryTask, JobScope, JobContext]]
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
                sweep_id=sweep_id,
                tokens=[TOKEN]
            )

            # TODO: Add job context, at minimum entity hash data. TBD how to get
            # this, could be Dynamo directly, or prepared by the sweep builder
            # and sent along
            job_context = JobContext()

            yield celery_task, job_scope, job_context


def run_tasks(sweep_id):
    cnt = 0
    for celery_task, job_scope, job_context in iter_tasks(sweep_id):
        celery_task.delay(job_scope, job_context)
        cnt += 1
    return cnt
