import functools

from common.celeryapp import get_celery_app
from oozer.common.facebook_async_report import FacebookReportDefinition
from .facebook_metrics_collector import collect_insights
from oozer.common.enum import ENTITY_ADACCOUNT

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
def fb_insights_adaccount_campaigns_lifetime(token, adaccount_id, job_context):
    """
    Collect insights for all campaigns under given adaccount

    :param string token:
    :param string adaccount_id:
    :param dict job_context:
    """
    report_definition = FacebookReportDefinition(
        level='campaign',
        date_preset='lifetime'
    )

    job_reporter = _bind_job_status_reporter(
        job_context['sweep_id'], job_context['report_type'], adaccount_id
    )

    collect_insights(
        token, adaccount_id, ENTITY_ADACCOUNT, report_definition, job_context,
        job_reporter
    )
