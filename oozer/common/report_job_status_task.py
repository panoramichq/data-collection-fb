from common.celeryapp import get_celery_app

app = get_celery_app()


@app.task
def report_job_status(*args, **kwargs):
    pass
