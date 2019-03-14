from facebook_business.adobjects.adaccount import AdAccount

from common.enums.reporttype import ReportType
from common.id_tools import generate_universal_id
from common.store.entities import AdAccountEntity
from common.store.scope import DEFAULT_SCOPE
from tests.base.testcase import TestCase, mock
from tests.base import random

from datetime import datetime

from oozer.common.cold_storage.batch_store import NormalStore
from oozer.common.enum import FB_ADACCOUNT_MODEL
from oozer.common.job_scope import JobScope
from common.enums.entity import Entity
from oozer.entities.collect_adaccount import collect_adaccount


class TestCollectAdAccount(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_fails_with_wrong_report_variant(self):
        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            entity_id=self.ad_account_id,
            tokens=['blah'],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=None,  # This actually should be set to AdAccount
            sweep_id='1'
        )

        with self.assertRaises(ValueError) as ex_trap:
            collect_adaccount(job_scope, None)

        assert 'Report level' in str(ex_trap.exception)

    def test_fails_without_a_token(self):
        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            entity_id=self.ad_account_id,
            tokens=[None],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.AdAccount,  # This actually should be set to AdAccount
            sweep_id='1'
        )

        with self.assertRaises(ValueError) as ex_trap:
            collect_adaccount(job_scope, None)

        assert 'token' in str(ex_trap.exception)

    def test_runs_correctly(self):
        account_id = random.gen_string_id()
        job_scope = JobScope(
            ad_account_id=self.ad_account_id,
            entity_id=self.ad_account_id,
            tokens=['A_REAL_TOKEN'],
            report_time=datetime.utcnow(),
            report_type='entity',
            report_variant=Entity.AdAccount,
            sweep_id='1'
        )

        universal_id_should_be = generate_universal_id(
            ad_account_id=self.ad_account_id,
            report_type=ReportType.entity,
            entity_id=self.ad_account_id,
            entity_type=Entity.AdAccount
        )

        account_data = AdAccount(
            fbid=account_id,
        )
        # Did not find a better way how to set this data on the inner AbstractCrudObject.
        timezone = 'Europe/Prague'
        account_data._data['timezone_name'] = timezone
        account_data._data['account_id'] = account_id

        with mock.patch.object(FB_ADACCOUNT_MODEL, 'remote_read', return_value=account_data), \
            mock.patch.object(NormalStore, 'store') as store:
            collect_adaccount(job_scope, None)

        assert store.called_with(account_data), 'Data should be stored with the cold store module'

        assert store.called
        store_args, store_keyword_args = store.call_args
        assert not store_keyword_args
        assert len(store_args) == 1, 'Store method should be called with just 1 parameter'

        data_actual = store_args[0]

        vendor_data_key = '__oprm'

        ad_account_dynamo = AdAccountEntity.get(DEFAULT_SCOPE, account_id)
        assert ad_account_dynamo.timezone == timezone
        assert ad_account_dynamo.ad_account_id == account_id

        assert vendor_data_key in data_actual and type(
            data_actual[vendor_data_key]) == dict, 'Special vendor key is present in the returned data'
        assert data_actual[vendor_data_key] == {
            'id': universal_id_should_be
        }, 'Vendor data is set with the right universal id'
