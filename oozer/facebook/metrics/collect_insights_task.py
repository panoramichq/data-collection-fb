import logging

from common.celeryapp import get_celery_app
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from .collect_insights import Insights


logger = logging.getLogger(__name__)


app = get_celery_app()


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def collect_insights_task(job_scope, job_context):
    """
    :param JobScope job_scope:
    :param JobContext job_context:
    """
    logger.info(
        f'{job_scope} started'
    )

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]
        # Note. we don't handle a situation here when
        # job still does not get a token. That's on purpose.
        # Job will check for a token again and if it finds none,
        # will generate appropriate reporting actions
        # Here we prep, but don't complain.

    cnt = 0
    data_iter = Insights.iter_collect_insights(
        job_scope, job_context
    )
    for datum in data_iter:
        cnt += 1
        if cnt % 100 == 0:
            logger.info(
                f'{job_scope} processed {cnt} data points so far'
            )

    logger.info(
        f'{job_scope} complete a total of {cnt} data points'
    )
