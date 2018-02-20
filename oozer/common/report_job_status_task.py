from common.celeryapp import get_celery_app

app = get_celery_app()


@app.task
def report_job_status(stage_id, job_scope, status_context=None):
    """
    We take job scope to divine basic information about the job itself.

    Stage id gives us the number we will be reporting.

    And status_context is some sort of dictionary that allows us to divine
    failure contexts

    :param stage_id:
    :param job_scope:
    :param status_context:
    :return:
    """
    status_context = status_context or {}

