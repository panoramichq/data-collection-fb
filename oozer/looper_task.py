import logging

from common.celeryapp import get_celery_app, RoutingKey

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
def sweep_looper_task(sweep_id):
    from oozer.looper import run_tasks
    logger.info(f"#{sweep_id}: Starting sweep loop")
    cnt = run_tasks(sweep_id)
    logger.info(f"#{sweep_id}: Apportioned {cnt} total jobs.")
