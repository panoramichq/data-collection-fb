import logging

from datetime import datetime, date

from boto3.resources.model import Action

from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.store.jobreport import JobReport
from oozer.common import cold_storage
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.job_scope import JobScope
from oozer.looper import SweepStatusTracker

logger = logging.getLogger(__name__)

_to_date_string_if_set = lambda v: v.strftime('%Y-%m-%d') if isinstance(v, (date, datetime)) else v


def _set_or_remove(attr, value=None) -> Action:
    """Set or remove value of model attribute."""
    if value is None:
        return attr.remove()

    return attr.set(value)


def _report_job_done_to_cold_store(job_scope):
    """
    :param JobScope job_scope:
    :return:
    """

    reporting_job_scope = JobScope(
        sweep_id=job_scope.sweep_id,
        ad_account_id=job_scope.ad_account_id,
        report_type=ReportType.sync_status
    )

    cold_storage.store(
        dict(
            job_id=job_scope.job_id,
            account_id=job_scope.ad_account_id,
            entity_type=job_scope.entity_type,
            entity_id=job_scope.entity_id,
            report_type=job_scope.report_type,
            report_variant=job_scope.report_variant,
            range_start=_to_date_string_if_set(job_scope.range_start),
            range_end=_to_date_string_if_set(job_scope.range_end),
            platform_namespace=job_scope.namespace,
        ),
        reporting_job_scope
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

    # defined status_bucket value means final status notification
    # derivative job descriptions should NOT count towards Sweep's tasks tracker
    if status_bucket is not None and not job_scope.is_derivative:
        with SweepStatusTracker(job_scope.sweep_id) as tracker:
            tracker.report_status(status_bucket)
            if status_bucket < 0:
                # one of those "i am still alive" status reports.
                logger.debug(
                    f'#{job_scope.sweep_id} Temporary status report "{job_scope.job_id}": "{status_bucket}"'
                )
            elif status_bucket > 0:
                # "terminal" Failed
                logger.warning(
                    f'#{job_scope.sweep_id} Failure status report "{job_scope.job_id}": "{status_bucket}"'
                )
            else:  # is zero
                # "terminal" Done
                logger.info(
                    f'#{job_scope.sweep_id} Done status report "{job_scope.job_id}": "{status_bucket}"'
                )

    is_done = False
    actions = None

    model = JobReport(job_scope.job_id)

    if stage_id == ExternalPlatformJobStatus.Done:
        actions = [
            _set_or_remove(model.last_success_dt, datetime.utcnow()),
            _set_or_remove(model.last_success_sweep_id, job_scope.sweep_id),
            _set_or_remove(model.last_total_running_time, job_scope.running_time),
            _set_or_remove(model.last_total_datapoint_count, job_scope.datapoint_count),
            _set_or_remove(model.fails_in_row, 0)
        ]
        is_done = True
    elif stage_id > 0:
        actions = [
            _set_or_remove(model.last_progress_dt, datetime.utcnow()),
            _set_or_remove(model.last_progress_stage_id, stage_id),
            _set_or_remove(model.last_progress_sweep_id, job_scope.sweep_id),
        ]
    elif stage_id < 0:
        actions = [
            _set_or_remove(model.last_failure_dt, datetime.utcnow()),
            _set_or_remove(model.last_failure_stage_id, stage_id),
            _set_or_remove(model.last_failure_sweep_id, job_scope.sweep_id),
            # last_failure_error=?
            _set_or_remove(model.last_failure_bucket, status_bucket),
            _set_or_remove(model.last_partial_running_time, job_scope.running_time),
            _set_or_remove(model.last_partial_datapoint_count, job_scope.datapoint_count),
            model.fails_in_row + 1
        ]

    if actions:
        model.update(actions=actions)

    if is_done and job_scope.namespace == JobScope.namespace:
        _report_job_done_to_cold_store(job_scope)
