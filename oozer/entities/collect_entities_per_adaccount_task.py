import logging

from common.celeryapp import get_celery_app
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope

from oozer.entities.twitter.collect_entities_per_adaccount import iter_collect_entities_per_adaccount


app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def collect_entities_per_adaccount_task(job_scope, job_context):
    """
    Collect all entities data for a given adaccount

    :param JobScope job_scope: The dict representation of JobScope
    :param JobContext job_context:
    """

    logger.info(
        f'{job_scope} started'
    )

    # FIXME: Use a token manager to query for auth (so far done manually in iter_collect_entities_per_adaccount)

    cnt = 0
    data_iter = iter_collect_entities_per_adaccount(
        job_scope, job_context
    )
    # we don't need the results here,
    # but need to spin the generator to work through entire list of work.
    # Generators are lazy. They don't do anything unless you consume from them
    for datum in data_iter:
        cnt += 1
        if cnt % 100 == 0:
            logger.info(
                f'{job_scope} processed {cnt} data points so far'
            )

    logger.info(
        f'{job_scope} complete a total of {cnt} data points'
    )
