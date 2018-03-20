import logging

from datetime import datetime

import config.application

from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.store.jobreport import JobReport
from oozer.common import cold_storage
from oozer.common.enum import FacebookJobStatus
from oozer.common.job_scope import JobScope


logger = logging.getLogger(__name__)


def report_job_status(stage_id, job_scope):
    """
    Report the job status to the job status store

    :param int|tuple stage_id: Either a scalar value with stage_id, or a
        tuple in the format of (stage_id, failure_bucket_id). This is used
        for convenience to communicate distinct failure "types"
    :param JobScope job_scope: The job scope that is attached to this particular
        report
    """

    # TODO: move this import to top of file then
    from oozer.looper import SweepStatusTracker

    status_bucket = FacebookJobStatus.failure_bucket_map.get(stage_id)  # allowed to be None and 0 (Success)

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
            else: # is zero
                # "terminal" Done
                logger.info(
                    f'#{job_scope.sweep_id} Done status report "{job_scope.job_id}": "{status_bucket}"'
                )

    if isinstance(stage_id, int):

        is_done = False
        data = None

        if stage_id == FacebookJobStatus.Done:
            data = dict(
                last_success_dt=datetime.utcnow(),
                last_success_sweep_id=job_scope.sweep_id,
            )
            is_done = True

        elif stage_id > 0:
            data = dict(
                last_progress_dt=datetime.utcnow(),
                last_progress_stage_id=stage_id,
                last_progress_sweep_id=job_scope.sweep_id
            )

        elif stage_id < 0:
            data = dict(
                last_failure_dt=datetime.utcnow(),
                last_failure_stage_id=stage_id,
                last_failure_sweep_id=job_scope.sweep_id,
                # last_failure_error=?
                last_failure_bucket=status_bucket
            )

        # important to use upsert, not .save()
        # .save() saves entire model, including zeroing out the optional fields
        # not set here. Must use blind update that updates only the fields we specify.
        if data:

            JobReport.upsert(
                job_scope.job_id,
                **data
            )

        # job_scope.namespace == JobScope.namespace means equal to default value
        # which is the value for external platform namespace
        # We don't want to report jobs for other namespaces (yet)
        if is_done and job_scope.namespace == JobScope.namespace:

            reporting_job_scope = JobScope(
                sweep_id=job_scope.sweep_id,
                namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
                ad_account_id=job_scope.ad_account_id,
                report_type=ReportType.sync_status
            )

            cold_storage.store(
                dict(
                    job_id=job_scope.job_id,
                    # status='done',  # discuss value and attr name with Mike
                    ad_account_id=job_scope.ad_account_id,
                    entity_type=job_scope.entity_type,
                    entity_id=job_scope.entity_id,
                    report_type=job_scope.report_type,
                    report_variant=job_scope.report_variant,
                    range_start=job_scope.range_start,
                    range_end=job_scope.range_end,
                    platform_namespace=job_scope.namespace,
                ),
                reporting_job_scope
            )
