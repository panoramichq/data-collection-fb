import logging

from datetime import datetime, date
from typing import Any

from boto3.resources.model import Action

from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.store.jobreport import JobReport
from oozer.common import cold_storage
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.job_scope import JobScope
from oozer.reporting import log_celery_task_status

logger = logging.getLogger(__name__)


def _to_date_string_if_set(v: Any) -> Any:
    return v.strftime('%Y-%m-%d') if isinstance(v, (date, datetime)) else v


def _set_or_remove(attr, value=None) -> Action:
    """Set or remove value of model attribute."""
    if value is None:
        return attr.remove()

    return attr.set(value)


def _report_job_done_to_cold_store(job_scope: JobScope):
    reporting_job_scope = JobScope(
        sweep_id=job_scope.sweep_id, ad_account_id=job_scope.ad_account_id, report_type=ReportType.sync_status
    )

    cold_storage.store(
        {
            'job_id': job_scope.job_id,
            'account_id': job_scope.ad_account_id,
            'entity_type': job_scope.entity_type,
            'entity_id': job_scope.entity_id,
            'report_type': job_scope.report_type,
            'report_variant': job_scope.report_variant,
            'range_start': _to_date_string_if_set(job_scope.range_start),
            'range_end': _to_date_string_if_set(job_scope.range_end),
            'platform_namespace': job_scope.namespace,
        },
        reporting_job_scope,
    )


def report_job_status(stage_id: int, job_scope: JobScope):
    """
    Report the job status to the job status store

    :param int stage_id: Scalar value with stage_id used
        for convenience to communicate distinct failure "types"
    :param JobScope job_scope: The job scope that is attached to this particular
        report
    """
    status_bucket = ExternalPlatformJobStatus.failure_bucket_map.get(stage_id)  # allowed to be None and 0 (Success)

    # Generic errors default bucket assignment
    if stage_id < 0 and status_bucket is None:
        status_bucket = FailureBucket.Other

    assert job_scope.sweep_id and job_scope.job_id, "Sweep or job id missing"

    is_done = False
    actions = None

    reported_state = None

    if stage_id == ExternalPlatformJobStatus.Done:
        actions = [
            JobReport.last_success_dt.set(datetime.utcnow()),
            JobReport.last_success_sweep_id.set(job_scope.sweep_id),
            JobReport.fails_in_row.remove(),
            _set_or_remove(JobReport.last_total_running_time, job_scope.running_time),
            _set_or_remove(JobReport.last_total_datapoint_count, job_scope.datapoint_count),
        ]
        is_done = True
        reported_state = 'succeeded'
    elif stage_id > 0:
        actions = [
            JobReport.last_progress_dt.set(datetime.utcnow()),
            JobReport.last_progress_stage_id.set(stage_id),
            JobReport.last_progress_sweep_id.set(job_scope.sweep_id),
        ]
        reported_state = 'in progress'
    elif stage_id < 0:
        actions = [
            JobReport.last_failure_dt.set(datetime.utcnow()),
            JobReport.last_failure_stage_id.set(stage_id),
            JobReport.last_failure_sweep_id.set(job_scope.sweep_id),
            JobReport.fails_in_row.add(1),
            # last_failure_error=?
            _set_or_remove(JobReport.last_failure_bucket, status_bucket),
            _set_or_remove(JobReport.last_partial_running_time, job_scope.running_time),
            _set_or_remove(JobReport.last_partial_datapoint_count, job_scope.datapoint_count),
        ]
        reported_state = 'failed'

    if actions:
        JobReport(job_scope.job_id).update(actions=actions)

    if is_done and job_scope.namespace == JobScope.namespace:
        _report_job_done_to_cold_store(job_scope)

    log_celery_task_status(job_scope, reported_state, status_bucket)
