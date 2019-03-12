import logging

from common.celeryapp import get_celery_app
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.entities.collect_entities_per_adaccount import iter_collect_entities_per_adaccount
from oozer.common.errors import CollectionError
from oozer.tasks import reported_task

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@reported_task
def collect_entities_per_adaccount_task(job_scope: JobScope, job_context: JobContext):
    """
    Collect all entities data for a given adaccount

    :param JobScope job_scope: The dict representation of JobScope
    :param JobContext job_context:
    """
    if not SweepRunningFlag.is_set(job_scope.sweep_id):
        logger.info(f'{job_scope} skipped because sweep {job_scope.sweep_id} is done')
        return

    logger.info(f'{job_scope} started')

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    data_iter = iter_collect_entities_per_adaccount(job_scope, job_context)
    cnt = 0
    try:
        for cnt, datum in enumerate(data_iter):
            if cnt % 100 == 0:
                logger.info(f'{job_scope} processed {cnt} data points so far')
    except Exception as e:
        raise CollectionError(cnt) from e

    logger.info(f'{job_scope} complete a total of {cnt} data points')
    return cnt
