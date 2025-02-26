# must be first, as it does event loop patching and other "first" things
from common.bugsnag import SEVERITY_ERROR
from common.error_inspector import ErrorTypesReport
from oozer.common.console_api import ConsoleApi
from oozer.entities.import_scope_entities_task import import_pages_task
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.tokens import PlatformTokenManager
from common.store.entities import PageEntity
from oozer.common.job_scope import JobScope
from oozer.common.enum import JobStatus
from oozer.common.report_job_status_task import report_job_status_task
from tests.base import random


class TestPageImportTask(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()

    def test_page_import_dies_on_missing_token(self):

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_pages,
            report_variant=Entity.Page,
            # Not passing tokens here
        )

        with mock.patch.object(report_job_status_task, 'delay') as status_task, mock.patch.object(
            PlatformTokenManager, 'get_best_token', return_value=None
        ) as get_best_token, mock.patch(
            'common.error_inspector.BugSnagContextData.notify'
        ) as bugsnag_notify, mock.patch(
            'common.error_inspector.API_KEY', 'something'
        ):

            # and code that falls back on PlatformTokenManager.get_best_token() gets nothing.

            import_pages_task(job_scope, None)
            assert bugsnag_notify.called
            actual_args, actual_kwargs = bugsnag_notify.call_args
            assert ' cannot proceed. No tokens provided.' in str(
                actual_args[0]
            ), 'Notify bugsnag correctly using correct Exception'
            assert {
                'severity': SEVERITY_ERROR,
                'job_scope': job_scope,
                'error_type': ErrorTypesReport.UNKNOWN,
            } == actual_kwargs, 'Notify bugsnag correctly'

        assert get_best_token.called

        # and must report failure status
        assert status_task.called
        aa, kk = status_task.call_args
        assert not kk
        assert aa == (JobStatus.GenericError, job_scope)

    def test_page_import(self):

        page_id = random.gen_string_id()

        pages = [dict(ad_account_id=page_id)]

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_accounts,
            report_variant=Entity.Page,
            tokens=['token'],
        )

        with mock.patch.object(ConsoleApi, 'get_pages', return_value=pages) as gp, mock.patch.object(
            PageEntity, 'upsert'
        ) as page_upsert, mock.patch.object(report_job_status_task, 'delay') as status_task, mock.patch(
            'oozer.entities.import_scope_entities_task._have_entity_access', return_value=True
        ) as _have_entity_access_mock:
            import_pages_task(job_scope, None)

        assert gp.called

        assert status_task.called
        # it was called many times, but we care about the last time only
        aa, kk = status_task.call_args
        assert not kk
        assert aa == (JobStatus.Done, job_scope)

        assert page_upsert.call_count == 1

        page_upsert_args = (
            (self.scope_id, page_id),
            {'is_active': True, 'updated_by_sweep_id': self.sweep_id, 'is_accessible': True},
        )

        args1 = page_upsert.call_args_list[0]

        assert args1 == page_upsert_args
