from common.facebook.enums.entity import Entity
from common.facebook.enums.reporttype import ReportType
from oozer.common.job_scope import JobScope
from oozer.facebook.entities import import_ad_accounts_task
from oozer.facebook.entities import collect_entities_per_adaccount_task
from oozer.facebook.metrics.tasks import collect_insights_task
from oozer.sync_expectations_task import sync_expectations_task


entity_report_handler_map = {
    ReportType.sync_expectations: {
        Entity.AdAccount: sync_expectations_task
    },
    ReportType.import_accounts: {
        Entity.Scope: import_ad_accounts_task,
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
