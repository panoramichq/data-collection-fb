import logging

from common.enums.failure_bucket import FailureBucket
from common.store.sweepentityreport import TwitterSweepEntityReport
from oozer.common.job_scope import JobScope
from oozer.common.enum import FacebookJobStatus


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
    # This will be refactored at the source
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

    TwitterSweepEntityReport(
        job_scope.sweep_id,
        job_scope.job_id,
        report_type=job_scope.report_type,
        ad_account_id=job_scope.ad_account_id,
        entity_id=job_scope.entity_id,
        entity_type=job_scope.entity_type,
        stage_id=stage_id,
        failure_bucket=status_bucket
    ).save()

    logger.debug(f"#:job {job_scope.job_id} reported completion stage {stage_id} (bucketed as {status_bucket})")
