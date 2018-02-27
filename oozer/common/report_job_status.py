import logging

from common.enums.failure_bucket import FailureBucket
from common.store.sweepentityreport import FacebookSweepEntityReport
from oozer.common.job_scope import JobScope


logger = logging.getLogger(__name__)


class JobStatus:
    """
    A class to represent job states (stage ids) for given jobs. Inherit and add
    your arbitrary states.

    The guideline is:

    - use positive numbers for "good" states
    - use negative numbers for "error" states

    You can write the job statues in two ways:

    either simple:

    MyStatus = 123

    or, compound with explicitly stated failure bucket, which is up to the
    task to decide, like this:

    ErrorStatus = -123, FailureBucket.Throttling


    You do not need to state FailureBucket.Other, as that is assumed to be the
    general case
    """

    Done = 1000
    """
    Any job considered done is represented by 1000
    """


def report_job_status(stage_status, job_scope):
    """
    Report the job status to the job status store

    :param int|tuple stage_status: Either a scalar value with stage_id, or a
        tuple in the format of (stage_id, failure_bucket_id). This is used
        for convenience to communicate distinct failure "types"
    :param JobScope job_scope: The job scope that is attached to this particular
        report
    """
    # This will be refactored at the souce
    # TODO: move this import to top of file then
    from oozer.looper import SweepStatusTracker

    failure_bucket = None

    # Unpack status and failure bucket
    if isinstance(stage_status, tuple):
        stage_id, failure_bucket = stage_status
    else:
        stage_id = stage_status

    # Generic errors default bucket assignment
    if stage_id < 0 and failure_bucket is None:
        failure_bucket = FailureBucket.Other

    # Sanity checks
    if stage_id >= 0:
        assert failure_bucket is None

    assert job_scope.sweep_id and job_scope.job_id, "Sweep or job id missing"

    if not job_scope.is_derivative:
        with SweepStatusTracker(job_scope.sweep_id) as tracker:
            tracker.report_status(failure_bucket)

    FacebookSweepEntityReport(
        job_scope.sweep_id, job_scope.job_id,
        report_type=job_scope.report_type,
        ad_account_id=job_scope.ad_account_id,
        entity_id=job_scope.entity_id,
        entity_type=job_scope.entity_type,
        stage_id=stage_id,
        failure_bucket=failure_bucket
    ).save()

    logger.debug(f"#: {stage_id} {job_scope.job_id} ({failure_bucket})")
