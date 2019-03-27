import logging

from common.celeryapp import get_celery_app
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.helpers import extract_tags_for_celery_fb_task
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.common.errors import CollectionError
from oozer.metrics.collect_organic_insights import InsightsOrganic
from oozer.reporting import reported_task

logger = logging.getLogger(__name__)
app = get_celery_app()


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task)
@Measure.counter(
    __name__, function_name_as_metric=True, count_once=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task
)
@reported_task
def collect_organic_insights_task(job_scope: JobScope, _: JobContext):
    if not SweepRunningFlag.is_set(job_scope.sweep_id):
        logger.info(f'{job_scope} skipped because sweep {job_scope.sweep_id} is done')
        return

    logger.info(f'{job_scope} started')

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    data_iter = InsightsOrganic.iter_collect_insights(job_scope)

    cnt = 0
    try:
        for _ in data_iter:
            cnt += 1
            if cnt % 100 == 0:
                logger.info(f'{job_scope} processed {cnt} data points so far')
    except Exception as e:
        raise CollectionError(e, cnt)

    logger.info(f'{job_scope} complete a total of {cnt} data points')
    return cnt
