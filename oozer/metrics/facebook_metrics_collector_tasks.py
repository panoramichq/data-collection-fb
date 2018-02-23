import functools

from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from oozer.common.facebook_async_report import FacebookReportDefinition
from .facebook_metrics_collector import collect_insights

app = get_celery_app()


@app.task
def fb_insights_adaccount_campaigns_lifetime(job_scope, job_context):
    """
    Collect insights for all campaigns under given adaccount

    :param JobScope job_scope: The dict representation of JobScope
    :param JobContext job_context: A job context we use for normative tasks
        reporting
    """

    # To conform to the effective task being executed, we use this wrapper to
    # setup the "basic" definition of the insights task, and that is insights
    # edge, target level, range and breakdown.

    # The other option would be to use job scope for everything, which
    # essentially means we are creating our own custom routing, albeit the
    # information to do that effectively is available in the JobScope object

    # The details, say which dates we collect for, are on the job scope
    report_definition = FacebookReportDefinition(
        level='campaign',
        date_preset='lifetime'
    )

    collect_insights(
        Entity.AdAccount, report_definition, job_scope, job_context
    )
