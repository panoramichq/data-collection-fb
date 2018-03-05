# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
from datetime import datetime
import pytz
import pytest

from oozer.common import job_scope
from common.enums.failure_bucket import FailureBucket
from common.enums.entity import Entity
from oozer.common.enum import FacebookJobStatus
from oozer.common.report_job_status import report_job_status
from common.store.sweepentityreport import FacebookSweepEntityReport


class TestReportJobStatus(TestCase):

    def _manufacture_job_scope(self):

        return job_scope.JobScope(
            ad_account_id='123',
            report_type='entity',
            report_time=datetime.now(pytz.utc),
            report_id="some_id",
            sweep_id='12',
            report_variant=Entity.Campaign
        )

    def test_basic_sweep_report_stored(self):
        ctx = self._manufacture_job_scope()

        report_job_status(123, ctx)

        stored_data = FacebookSweepEntityReport.get(ctx.sweep_id, ctx.job_id)

        assert stored_data.to_dict() == {
            'sweep_id': '12',
            'job_id': 'fb:123:::entity:C',
            'ad_account_id': '123',
            'stage_id': 123,
            'entity_id': None,
            'entity_type': None,
            'failure_bucket': None,
            'failure_error': None,
            'report_type': 'entity',
        }

    def test_failure_bucket(self):

        ctx = self._manufacture_job_scope()
        report_job_status(FacebookJobStatus.ThrottlingError, ctx)

        stored_data = FacebookSweepEntityReport.get(ctx.sweep_id, ctx.job_id)

        assert stored_data.to_dict() == {
            'sweep_id': '12',
            'job_id': 'fb:123:::entity:C',
            'ad_account_id': '123',
            'entity_id': None,
            'stage_id': FacebookJobStatus.ThrottlingError,
            'entity_type': None,
            'failure_bucket': FailureBucket.Throttling,
            'failure_error': None,
            'report_type': 'entity',
        }
