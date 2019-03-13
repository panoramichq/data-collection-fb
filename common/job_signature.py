from typing import Callable


class JobSignature:
    """Represent serialized call signature of a task."""

    def __init__(self, job_id: str = None, job_id_func: Callable[[], str] = None):
        self._job_id = job_id
        self._job_id_func = job_id_func

    @classmethod
    def bind(cls, job_id: str, *args, **kwargs):
        return cls(job_id=job_id, *args, **kwargs)

    @property
    def job_id(self):
        """Lazily generate job_id if needed."""
        if self._job_id is None:
            self._job_id = self._job_id_func()

        return self._job_id
