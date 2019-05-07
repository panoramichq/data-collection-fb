import contextlib
from unittest.mock import patch

from oozer.common.sorted_jobs_queue import SortedJobsQueue
from oozer.producer import TaskProducer


@contextlib.contextmanager
def mock_reader(_):
    yield [
        ('fb|029dc5f3253c456ea5ee29d0919b686e|||dayplatform|A|2000-01-02', {}, 100)
    ]


@patch.object(SortedJobsQueue, 'JobsReader', new=mock_reader)
def test_iter_tasks():
    producer = TaskProducer('sweep-id')

    for (task, job_scope, job_context, score) in producer.iter_tasks():
        assert task is not None
        assert job_scope.job_id == 'fb|029dc5f3253c456ea5ee29d0919b686e|||dayplatform|A|2000-01-02'
        assert score == 100
