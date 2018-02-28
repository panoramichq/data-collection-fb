import logging

from common.celeryapp import get_celery_app, RoutingKey


app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
def echo(message='This is Long-Running queue'):
    print(message)


@app.task(routing_key=RoutingKey.longrunning)
def sweep_builder_task(sweep_id=None, start_looper=True):

    from datetime import datetime
    from oozer.tasks import sweep_looper_task
    from .persister import iter_persist_prioritized

    sweep_id = sweep_id or datetime.utcnow().strftime('%Y%m%d%H%M%S')

    logger.info(f"#{sweep_id} Starting sweep")

    cnt = 0
    for claim in iter_persist_prioritized(sweep_id):
        cnt += 1
        if cnt % 100 == 0:
            logger.info(f'#{sweep_id}: Queueing up #{cnt}')

    logger.info(f"#{sweep_id}: Queued up a total of {cnt} tasks")

    if start_looper:
        sweep_looper_task.delay(sweep_id)
