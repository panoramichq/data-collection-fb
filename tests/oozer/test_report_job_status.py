# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.enums.failure_bucket import FailureBucket
from oozer.common.report_job_status import JobStatus


class TestReportJobStatus(TestCase):

    def test_something(self):

        assert True
