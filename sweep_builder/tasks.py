import logging

from common.celeryapp import get_celery_app, RoutingKey


app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
def echo(message='This is Long-Running queue'):
    print(message)


@app.task(routing_key=RoutingKey.longrunning)
def sweep_builder_start():
    from datetime import datetime
    from pytz import UTC
    from common.tztools import dt_to_timestamp
    from .persister import iter_persist_prioritized

    sweep_id = dt_to_timestamp(
        datetime.now().replace(tzinfo=UTC)
    )

    cnt = 0
    for claim in iter_persist_prioritized(sweep_id):
        cnt += 1
        if cnt % 100 == 0:
            logger.info(f'Queued up claim #{cnt}')
