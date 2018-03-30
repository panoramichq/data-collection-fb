# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.tokens import PlatformTokenManager
from common.store.entities import AdAccountEntity
from oozer.common.console_api import ConsoleApi
from oozer.common.job_scope import JobScope
from oozer.common.enum import JobStatus
from oozer.common.report_job_status_task import report_job_status_task
from oozer.entities.import_ad_accounts_task import import_ad_accounts_task, scope_api_map
from tests.base import random


class TestAdAccountImportTask(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()

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

        ad_account_id = random.gen_string_id()
        tz = 'SovietAmerica/Cheburashka_grad'

        accounts = [
            dict(
                ad_account_id = ad_account_id,
                timezone = tz
            )
        ]

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            entity_type=Entity.Scope,
            entity_id=self.scope_id,
            report_type=ReportType.import_accounts,
            report_variant=Entity.AdAccount,
            tokens=['token']
        )

        with mock.patch.object(ConsoleApi, 'get_active_accounts', return_value=accounts) as gaa, \
            mock.patch.dict(scope_api_map, {self.scope_id: ConsoleApi}), \
            mock.patch.object(AdAccountEntity, 'upsert') as aa_upsert, \
            mock.patch.object(report_job_status_task, 'delay') as status_task:

            import_ad_accounts_task(job_scope, None)

        assert gaa.called

        assert status_task.called
        # it was called many times, but we care about the last time only
        aa, kk = status_task.call_args
        assert not kk
        assert aa == (JobStatus.Done, job_scope)

        assert aa_upsert.called
        aa, kk = aa_upsert.call_args
        assert aa == (
            self.scope_id,
            ad_account_id
        )
        assert kk == {
            'is_active': True,
            'timezone': tz,
            'updated_by_sweep_id': self.sweep_id
        }
