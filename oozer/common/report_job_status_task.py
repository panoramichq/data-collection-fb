from common.celeryapp import get_celery_app
from oozer.common.job_scope import JobScope


app = get_celery_app()


@app.task
def report_job_status_task(stage_status, job_scope):
    """
    We take job scope to divine basic information about the job itself.

    Stage id gives us the number we will be reporting.

    And status_context is some sort of dictionary that allows us to divine
    failure contexts

    :param int stage_status:
    :param JobScope job_scope:
    :return:
    """
    from .report_job_status import report_job_status
    report_job_status(stage_status, job_scope)
