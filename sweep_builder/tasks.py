import logging

from common.celeryapp import get_celery_app, RoutingKey
from common.measurement import Measure
from sweep_builder.init_tokens import init_tokens


app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def echo(message='This is Long-Running queue'):
    print(message)


@app.task(routing_key=RoutingKey.longrunning)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def sweep_builder_task(sweep_id=None, start_looper=True):

    from datetime import datetime
    from oozer.tasks import sweep_looper_task
    from .persister import iter_persist_prioritized

    sweep_id = sweep_id or datetime.utcnow().strftime('%Y%m%d%H%M%S')

    # In the jobs persister we purposefully avoid persisting
    # anything besides the Job ID. This means that things like tokens
    # and other data on *Claim is lost.
    # As long as we are doing that, we need to leave tokens somewhere
    # for workers to pick up.
    init_tokens(sweep_id)

    logger.info(f"#{sweep_id} Starting sweep")

    cnt = 0
    for claim in iter_persist_prioritized(sweep_id):
        cnt += 1
        if cnt % 100 == 0:
            logger.info(f'#{sweep_id}: Queueing up #{cnt}')

    logger.info(f"#{sweep_id}: Queued up a total of {cnt} tasks")

    if start_looper:
        sweep_looper_task.delay(sweep_id)
