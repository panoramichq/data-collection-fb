from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from oozer.common.job_scope import JobScope
from oozer.entities.extract_adaccounts_task import extract_adaccounts_task
from oozer.entities.tasks import collect_entities_per_adaccount_task
from oozer.metrics.tasks import collect_insights_task


entity_report_handler_map = {
    ReportType.console: {
        Entity.AdAccount: extract_adaccounts_task,
    },
    ReportType.entities: {
        Entity.AdAccount: None, # FIXME: Fetch ad accounts from facebook - regular pipeline
        Entity.Campaign: collect_entities_per_adaccount_task,
        Entity.AdSet: collect_entities_per_adaccount_task,
        Entity.Ad: collect_entities_per_adaccount_task,
    },
    ReportType.lifetime: {
        Entity.Campaign: collect_insights_task,
        Entity.AdSet: collect_insights_task,
        Entity.Ad: collect_insights_task,
    },
    ReportType.day_age_gender: {
        Entity.Ad: collect_insights_task
    },
    ReportType.day_dma: {
        Entity.Ad: collect_insights_task
    },
    ReportType.day_hour: {
        Entity.Ad: collect_insights_task
    },
    ReportType.day_platform: {
        Entity.Ad: collect_insights_task
    }
}


def resolve_job_scope_to_celery_task(job_scope):
    """
    Given parameters of the jon expressed in JobScope object
    returns registered Celery task handler for the job.

    Returns None if no handler for such JobScope is registered

    :param JobScope job_scope:
    :type JobScope: callable or None
    :return:
    """
    return entity_report_handler_map.get(
        job_scope.report_type, {}
    ).get(
        job_scope.entity_type or job_scope.report_variant
    )
