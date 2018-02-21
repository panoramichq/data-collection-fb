import logging

from common.celeryapp import get_celery_app, RoutingKey

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
def sweep_looper_task(sweep_id):
    from oozer.looper import iter_tasks
    logger.info(f"#{sweep_id}: Starting sweep loop")
    cnt = 0
    for job_id in iter_tasks(sweep_id):
        cnt += 1
        if cnt % 100 == 0:
            logger.info(f'#{sweep_id}: Apportioned {cnt} jobs so far.')
    logger.info(f"#{sweep_id}: Apportioned {cnt} total jobs.")
