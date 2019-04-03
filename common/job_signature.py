class JobSignature:
    """Represent serialized call signature of a task."""

    __slots__ = ['job_id']

    def __init__(self, job_id: str):
        self.job_id = job_id

    def __hash__(self):
        return hash(self.job_id)

    def __ne__(self, other):
        return not self == other

    def __eq__(self, other):
        return isinstance(other, JobSignature) and other.job_id == self.job_id
