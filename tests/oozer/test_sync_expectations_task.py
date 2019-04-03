# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id, parse_id_parts
from common.tztools import now
from oozer.common import cold_storage
from oozer.common import expecations_store
from oozer.common.job_scope import JobScope
from oozer.common.enum import JobStatus
from oozer import sync_expectations_task
from tests.base import random


class TestSyncExcpectationsTask(TestCase):
    """
    Tests that looper code will recognize the types of reports we expect it to
    recognize from the job ID
    """

    def test_task_complains_about_bad_report_type(self):

        sync_expectations_job_scope = JobScope(
            sweep_id=random.gen_string_id(),
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.lifetime,  # <----------- this is wrong
        )

        with self.assertRaises(AssertionError) as ex_catcher:
            sync_expectations_task.sync_expectations(sync_expectations_job_scope)

        assert 'Only sync_expectations report' in str(ex_catcher.exception)

    def test_task_does_not_blow_up(self):
        # this is almost same thing as the next test
        # where we check that call signature is right,
        # but when call signature changes and our tests don't,
        # it becomes irrelevant if we have tests - they check for wrong thing
        # So, here we actually call "store" and in next test
        # we intercept the call and check payload.
        # Don't remove me. Not duplicate.

        expectation_job_id = generate_id(
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.day_hour,
            report_variant=Entity.Ad,
            range_start='2000-01-01',
        )
        rr = [expectation_job_id]

        sync_expectations_job_scope = JobScope(
            sweep_id=random.gen_string_id(),
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.sync_expectations,
        )

        with mock.patch.object(expecations_store, 'iter_expectations_per_ad_account', return_value=rr):
            sync_expectations_task.sync_expectations(sync_expectations_job_scope)

    def test_task_is_called_with_right_data(self):

        range_start = now()
        range_start_should_be = range_start.strftime('%Y-%m-%d')

        expected_job_id = generate_id(
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.day_hour,
            report_variant=Entity.Ad,
            range_start=range_start,
        )
        rr = [expected_job_id]
        expected_job_id_parts = parse_id_parts(expected_job_id)

        sync_expectations_job_scope = JobScope(
            sweep_id=random.gen_string_id(),
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.sync_expectations,
        )

        with mock.patch.object(
            expecations_store, 'iter_expectations_per_ad_account', return_value=rr
        ) as jid_iter, mock.patch.object(cold_storage.ChunkDumpStore, 'store') as store:

            sync_expectations_task.sync_expectations(sync_expectations_job_scope)

        assert jid_iter.called
        aa, kk = jid_iter.call_args
        assert not kk
        assert aa == (sync_expectations_job_scope.ad_account_id, sync_expectations_job_scope.sweep_id)

        assert store.called
        aa, kk = store.call_args
        assert not kk
        assert len(aa) == 1

        data = aa[0]

        assert data == {
            'job_id': expected_job_id,
            # missing "ad_" is intentional.
            # this matches this attr name as sent by FB
            # and ysed by us elsewhere in the company
            'account_id': expected_job_id_parts.ad_account_id,
            'entity_type': expected_job_id_parts.entity_type,
            'entity_id': expected_job_id_parts.entity_id,
            'report_type': expected_job_id_parts.report_type,
            'report_variant': expected_job_id_parts.report_variant,
            'range_start': range_start_should_be,  # checking manually to ensure it's properly stringified
            'range_end': None,
            'platform_namespace': JobScope.namespace,  # default platform value
        }

    def test_task_error_is_logged_into_job_report(self):
        from oozer.common.report_job_status_task import report_job_status_task

        class MyException(Exception):
            pass

        sync_expectations_job_scope = JobScope(
            sweep_id=random.gen_string_id(),
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.sync_expectations,
        )

        with mock.patch.object(report_job_status_task, 'delay') as job_report, mock.patch.object(
            sync_expectations_task, 'sync_expectations', side_effect=MyException('nope!')
        ):

            with self.assertRaises(MyException):
                sync_expectations_task.sync_expectations_task.delay(sync_expectations_job_scope, None)

        assert job_report.called

        aa, kk = job_report.call_args

        assert not kk
        code, job_scope_actual = aa
        assert code < 0  # some sort of *Failure* code
        assert job_scope_actual == sync_expectations_job_scope

    def test_task_success_is_logged_into_job_report(self):
        from oozer.common.report_job_status_task import report_job_status_task

        sync_expectations_job_scope = JobScope(
            sweep_id=random.gen_string_id(),
            ad_account_id=random.gen_string_id(),
            report_type=ReportType.sync_expectations,
        )

        with mock.patch.object(report_job_status_task, 'delay') as job_report:

            sync_expectations_task.sync_expectations_task.delay(sync_expectations_job_scope, None)

        assert job_report.called

        aa, kk = job_report.call_args

        assert not kk
        code, job_scope_actual = aa
        assert code == JobStatus.Done
        assert job_scope_actual == sync_expectations_job_scope
