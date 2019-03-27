from datetime import datetime, date

from common.celeryapp import get_celery_app
from common.id_tools import parse_id_parts
from common.bugsnag import BugSnagContextData
from common.measurement import Measure
from common.enums.reporttype import ReportType
from common.enums.entity import Entity
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.common.enum import JobStatus
from oozer.common import expecations_store
from oozer.common.helpers import extract_tags_for_celery_fb_task
from oozer.common.job_scope import JobScope

app = get_celery_app()


def _to_date_string_if_set(v):
    return v.strftime('%Y-%m-%d') if isinstance(v, (date, datetime)) else v


def sync_expectations(job_scope: JobScope):
    assert (
        job_scope.report_type == ReportType.sync_expectations
    ), 'Only sync_expectations report type is processed by this task'

    if job_scope.ad_account_id:
        # this is per AA task. No need to iterate over all
        ad_account_ids_iter = [job_scope.ad_account_id]
    else:
        ad_account_ids_iter = expecations_store.iter_expectations_ad_accounts(sweep_id=job_scope.sweep_id)

    for ad_account_id in ad_account_ids_iter:

        ad_account_scoped_job_scope = JobScope(
            job_scope.to_dict(), ad_account_id=ad_account_id, entity_type=Entity.AdAccount, entity_id=ad_account_id
        )

        with ChunkDumpStore(ad_account_scoped_job_scope, chunk_size=200) as store:

            job_ids_iter = expecations_store.iter_expectations_per_ad_account(
                ad_account_id, ad_account_scoped_job_scope.sweep_id
            )

            for job_id in job_ids_iter:
                job_id_parts = parse_id_parts(job_id)

                # default is platform namespace and we communicate out only those
                if job_id_parts.namespace == JobScope.namespace:
                    store(
                        {
                            'job_id': job_id,
                            # 'status':'expected',
                            'account_id': job_id_parts.ad_account_id,
                            'entity_type': job_id_parts.entity_type,
                            'entity_id': job_id_parts.entity_id,
                            'report_type': job_id_parts.report_type,
                            'report_variant': job_id_parts.report_variant,
                            'range_start': _to_date_string_if_set(job_id_parts.range_start),
                            'range_end': _to_date_string_if_set(job_id_parts.range_end),
                            'platform_namespace': job_id_parts.namespace,
                        }
                    )


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task)
@Measure.counter(
    __name__, function_name_as_metric=True, count_once=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task
)
def sync_expectations_task(job_scope: JobScope, _):
    from oozer.common.report_job_status_task import report_job_status_task

    try:
        sync_expectations(job_scope)
    except Exception as ex:
        BugSnagContextData.notify(ex, job_scope=job_scope)

        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(JobStatus.GenericError, job_scope)
        raise
    report_job_status_task.delay(JobStatus.Done, job_scope)
