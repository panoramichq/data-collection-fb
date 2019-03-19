
class JobSignature:
    """Represent serialized call signature of a task."""

    __slots__ = ['job_id']

    def __init__(self, job_id: str):
        self.job_id = job_id
