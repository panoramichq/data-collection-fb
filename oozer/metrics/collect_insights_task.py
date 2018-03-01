import logging

from common.celeryapp import get_celery_app
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from .collect_insights import iter_collect_insights


logger = logging.getLogger(__name__)


app = get_celery_app()


@app.task
def collect_insights_task(job_scope, job_context):
    """
    :param JobScope job_scope:
    :param JobContext job_context:
    """
    logger.info(
        f'{job_scope} started'
    )

    cnt = 0
    data_iter = iter_collect_insights(
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
