from oozer.common.job_scope import JobScope


class CollectionError(Exception):
    """Unrecoverable error thrown when a collection job fails."""

    def __init__(self, inner: Exception, partial_datapoint_count: int):
        self.inner = inner
        self.partial_datapoint_count = partial_datapoint_count


class TimeoutException(Exception):

    """Raised when a task times out - see @timeout decorator."""


class TaskOutsideSweepException(Exception):

    """Raised when a task starts after sweep stopped running."""

    def __init__(self, job_scope: JobScope):
        self.job_scope = job_scope
