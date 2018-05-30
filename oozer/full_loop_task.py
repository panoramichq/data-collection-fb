from common.celeryapp import get_celery_app
from oozer.full_loop import run_sweeps_forever

app = get_celery_app()

@app.task()
def run_sweeps_forever_task():
    run_sweeps_forever()
