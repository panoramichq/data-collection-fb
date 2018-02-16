import functools

from common.celeryapp import get_celery_app
from oozer.collector import collect_entities_for_adaccount, collect_insights, \
    ENTITY_ADACCOUNT, ENTITY_CAMPAIGN, FacebookReportDefinition

app = get_celery_app()


# TODO: I still feel the job status report / entity feedback binding is kind of
# verbose and ugly.


def _bind_job_status_reporter(*args):
    """
    A helper method to bind the job status reporter task to arguments and celery
    delay.

    This way, the individual tasks do not need to care about how specifically
    the job report task is prepared

    :param list args: Arbitrary arguments
    :return callable: The bound method
    """
    return functools.partial(report_job_status.delay, *args)


def _bind_entity_feedbacker():
    """
    A helper method to bind the entity feedbacker task to arguments and celery
    delays

    This way, the individual tasks do not need to care about how specifically
    the feedback entity task is prepared

    :return callable: The bound method
    """
    return functools.partial(feedback_entity.delay)


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
    job_reporter = _bind_job_status_reporter(
        job_context['sweep_id'], job_context['report_type'], adaccount_id
    )

    collect_entities_for_adaccount(
        token, adaccount_id, ENTITY_CAMPAIGN, job_context,
        context, job_reporter, _bind_entity_feedbacker()
    )


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


@app.task
def feedback_entity(entity, entity_hash):
    """
    This task is to feedback information about entity collected by updating
    data store.

    <TBW>
    """
    id, ad_account_id = entity['id']

    # For given plaftorm and given entity type

    # - fetch entity id
    # - get parent ad account id

    # Write to entity model table:
    #  - ad account id
    #  - entity id
    #  - EOL
    #  - BOL
    #  - checksum
    #  - fields checksum


@app.task
def report_job_status(
    sweep_id, report_type, adaccount_id, entity_type, entity_id, stage_id,
    stage_context
):
    """
    This task is to store information about particular sweep / job

    <TBW>

    """
    pass
