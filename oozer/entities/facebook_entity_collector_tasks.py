import functools

from common.celeryapp import get_celery_app

from .facebook_entity_collector import (
    collect_entities_for_adaccount,
    ENTITY_CAMPAIGN
)


app = get_celery_app()


def _bind_job_status_reporter(*args):
    """
    A helper method to bind the job status reporter task to arguments and celery
    delay.

    This way, the individual tasks do not need to care about how specifically
    the job report task is prepared

    :param list args: Arbitrary arguments
    :return callable: The bound method
    """
    from oozer.common.tasks import report_job_status
    return functools.partial(report_job_status.delay, *args)


@app.task
def fb_entities_adaccount_campaigns(
        token, adaccount_id, job_context, context,
):
    """
    Collect all campaign entities for a given adaccount

    :param string token:
    :param string adaccount_id:
    :param dict job_context:
    :param dict context:
    """
    from oozer.entities.tasks import entity_feedback_task

    job_reporter = _bind_job_status_reporter(
        job_context['sweep_id'], job_context['report_type'], adaccount_id
    )

    collect_entities_for_adaccount(
        token, adaccount_id, ENTITY_CAMPAIGN, job_context,
        context, job_reporter, entity_feedback_task.delay
    )
