import logging

logger = logging.getLogger(__name__)


class JobStatus:
    """
    A class to represent job states (stage ids) for given jobs. Inherit and add
    your arbitrary states.

    The guideline is:

    - use positive numbers for "good" states
    - use negative numbers for "error" states
    """

    Done = 1000
    """
    Any job considered done is represented by 1000
    """

    @classmethod
    def as_status_context_dict(cls):
        """
        Collapse all the negative statuses on the class into failure buckets
        (for now), so that the report_job can look them up

        If this proves to be meaningful for "good" states, we can extend this
        to them too.

        :return dict: The error -> failure bucket map
        """
        return {

        }


def report_job_status(stage_id, job_scope, status_context=None):
    """

    :param stage_id:
    :param job_scope:
    :param status_context:
    :return:
    """
    logger.warning(f"#: {stage_id} {job_scope.job_id}")
    status_context = status_context or {}
