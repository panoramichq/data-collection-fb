import logging
from common.enums.failure_bucket import FailureBucket

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
    failure_bucket = None

    # Unpack status and failure bucket
    if isinstance(stage_status, tuple):
        stage_id, failure_bucket = stage_status
    else:
        stage_id = stage_status

    # Generic errors default bucket assignment
    if stage_id < 0 and failure_bucket is None:
        failure_bucket = FailureBucket.Other

    # Sanity check
    if stage_id >= 0:
        assert failure_bucket is None

    logger.warning(f"#: {stage_id} {job_scope.job_id} ({failure_bucket})")
