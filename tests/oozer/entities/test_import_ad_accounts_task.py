# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.tokens import PlatformTokenManager
from common.store.entities import AdAccountEntity, PageEntity
from oozer.common.console_api import ConsoleApi
from oozer.common.job_scope import JobScope
from oozer.common.enum import JobStatus
from oozer.common.report_job_status_task import report_job_status_task
from oozer.entities.import_scope_entities_task import import_ad_accounts_task, _get_entities_to_import, \
    import_pages_task
from tests.base import random


class TestAdAccountImportTask(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()

    def _ad_acc(self, entity_id, active):
        return dict(ad_account_id=entity_id, active=active)

    def test_aa_import_dies_on_missing_token(self):

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_accounts,
            report_variant=Entity.AdAccount,
            # Not passing tokens here
        )

        with mock.patch.object(report_job_status_task, 'delay') as status_task, \
            mock.patch.object(PlatformTokenManager, 'get_best_token', return_value=None) as get_best_token, \
            self.assertRaises(ValueError) as ex_trap:

            # and code that falls back on PlatformTokenManager.get_best_token() gets nothing.

            import_ad_accounts_task(job_scope, None)

        # so, it must complain specifically about tokens
        assert 'cannot proceed. No tokens' in str(ex_trap.exception)

        assert get_best_token.called

        # and must report failure status
        assert status_task.called
        aa, kk = status_task.call_args
        assert not kk
        assert aa == (
            JobStatus.GenericError,
            job_scope
        )

    def test_aa_import(self):

        active_ad_account_id = random.gen_string_id()
        inactive_ad_account_id = random.gen_string_id()

        accounts = [
            dict(
                ad_account_id=active_ad_account_id,
                active=True,
            ),
            dict(
                ad_account_id=inactive_ad_account_id,
                active=False,
            ),
        ]

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_accounts,
            report_variant=Entity.AdAccount,
            tokens=['token']
        )

        with mock.patch.object(ConsoleApi, 'get_accounts', return_value=accounts) as gaa, \
            mock.patch.object(AdAccountEntity, 'upsert') as aa_upsert, \
            mock.patch.object(report_job_status_task, 'delay') as status_task:

            import_ad_accounts_task(job_scope, None)

        assert gaa.called

        assert status_task.called
        # it was called many times, but we care about the last time only
        aa, kk = status_task.call_args
        assert not kk
        assert aa == (JobStatus.Done, job_scope)

        assert aa_upsert.call_count == 2

        active_account_upsert_args = (
            (self.scope_id, active_ad_account_id),
            {
                'is_active': True,
                'updated_by_sweep_id': self.sweep_id
            }
        )

        inactive_account_upsert_args = (
            (self.scope_id, inactive_ad_account_id),
            {
                'is_active': False,
                'updated_by_sweep_id': self.sweep_id
            }
        )

        args1, args2 = aa_upsert.call_args_list

        assert args1 == active_account_upsert_args
        assert args2 == inactive_account_upsert_args

    def test_pages_import(self):

        active_page_id = random.gen_string_id()
        inactive_page_id = random.gen_string_id()

        pages = [
            dict(
                ad_account_id=active_page_id,
                active=True,
            ),
            dict(
                ad_account_id=inactive_page_id,
                active=False,
            ),
        ]

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_pages,
            report_variant=Entity.Page,
            tokens=['token']
        )

        with mock.patch.object(ConsoleApi, 'get_pages', return_value=pages) as gp, \
            mock.patch.object(PageEntity, 'upsert') as pg_upsert, \
            mock.patch.object(report_job_status_task, 'delay') as status_task:

            import_pages_task(job_scope, None)

        assert gp.called

        assert status_task.called
        # it was called many times, but we care about the last time only
        pg, kk = status_task.call_args
        assert not kk
        assert pg == (JobStatus.Done, job_scope)

        assert pg_upsert.call_count == 2

        active_page_upsert_args = (
            (self.scope_id, active_page_id),
            {
                'is_active': True,
                'updated_by_sweep_id': self.sweep_id
            }
        )

        inactive_account_upsert_args = (
            (self.scope_id, inactive_page_id),
            {
                'is_active': False,
                'updated_by_sweep_id': self.sweep_id
            }
        )

        args1, args2 = pg_upsert.call_args_list

        assert args1 == active_page_upsert_args
        assert args2 == inactive_account_upsert_args


    def test__get_entities_to_import(self):
        accounts = [
            self._ad_acc('1', True),
            self._ad_acc('2', False),
            self._ad_acc('3', True),
            self._ad_acc('3', False),
            self._ad_acc('4', False),
        ]
        result = list(_get_entities_to_import(accounts, 'ad_account_id'))
        expected = [
            self._ad_acc('1', True),
            self._ad_acc('2', False),
            self._ad_acc('3', True),
            self._ad_acc('4', False),
        ]
        self.assertEqual(result, expected)
