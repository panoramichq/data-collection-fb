# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.tztools import now
from oozer.common import cold_storage
from oozer.common import report_job_status
from oozer.common.job_scope import JobScope
from tests.base import random


class TestJobDoneReporter(TestCase):

    maxDiff = None

    def test_right_data_is_communicated_on_done_signal(self):

        range_start = now()
        range_start_should_be = range_start.strftime('%Y-%m-%d')

        job_scope = JobScope(
            sweep_id=random.gen_string_id(),
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.day_hour,
            report_variant=Entity.Ad,
            range_start=now()
        )

        with mock.patch.object(cold_storage, 'store') as store:
            report_job_status._report_job_done_to_cold_store(job_scope)

        assert store.called
        aa, kk = store.call_args
        assert not kk
        data, job_scope_reported = aa

        assert data == {
            'job_id': job_scope.job_id,
            # missing "ad_" is intentional.
            # this matches this attr name as sent by FB
            # and ysed by us elsewhere in the company
            'account_id': job_scope.ad_account_id,
            'entity_type': None,
            'entity_id': None,
            'report_type': ReportType.day_hour,
            'report_variant': Entity.Ad,
            'range_start': range_start_should_be,
            'range_end': None,
            'platform_namespace': JobScope.namespace  # default platform value
        }

        assert job_scope_reported.sweep_id == job_scope.sweep_id
        assert job_scope_reported.ad_account_id == job_scope.ad_account_id
        assert job_scope_reported.report_type == ReportType.sync_status
