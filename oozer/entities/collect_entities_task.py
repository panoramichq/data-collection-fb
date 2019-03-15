import logging
from typing import Generator

from common.celeryapp import get_celery_app
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.errors import CollectionError
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.entities.collect_entities_iterators import (
    iter_collect_entities_per_page_post,
    iter_collect_entities_per_adaccount,
    iter_collect_entities_per_page,
)
from oozer.reporting import reported_task

logger = logging.getLogger(__name__)
app = get_celery_app()


def collect_entities_from_iterator(job_scope: JobScope, entity_iterator: Generator[object, None, None]) -> int:
    if not SweepRunningFlag.is_set(job_scope.sweep_id):
        logger.info(f'{job_scope} skipped because sweep {job_scope.sweep_id} is done')
        raise CollectionError(Exception(f'{job_scope} skipped because sweep {job_scope.sweep_id} is done'), 0)

    logger.info(f'{job_scope} started')

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    cnt = 0
    try:
        for cnt, datum in enumerate(entity_iterator):
            if cnt % 100 == 0:
                logger.info(f'{job_scope} processed {cnt} data points so far')
    except Exception as e:
        raise CollectionError(e, cnt)

    logger.info(f'{job_scope} complete a total of {cnt} data points')
    return cnt


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@reported_task
def collect_entities_per_adaccount_task(job_scope: JobScope, _: JobContext):
    """
    Collect all entities data for a given adaccount
    """
    return collect_entities_from_iterator(job_scope, iter_collect_entities_per_adaccount(job_scope))


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@reported_task
def collect_entities_per_page_task(job_scope: JobScope, _: JobContext):
    """
    Collect all entities data for a given page
    """
    return collect_entities_from_iterator(job_scope, iter_collect_entities_per_page(job_scope))


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@reported_task
def collect_entities_per_page_post_task(job_scope: JobScope, _: JobContext):
    """
    Collect all entities data for a given page
    """
    return collect_entities_from_iterator(job_scope, iter_collect_entities_per_page_post(job_scope))
