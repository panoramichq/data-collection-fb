# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_universal_id
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.common.job_scope import JobScope
from oozer.entities.collect_entities_per_adaccount import iter_collect_entities_per_adaccount
from oozer.entities.collect_entities_per_adaccount import FB_AD_MODEL, FB_ADSET_MODEL, FB_CAMPAIGN_MODEL, FB_ADACCOUNT_MODEL

from tests.base import random


class TestCollectEntitiesPerAdAccount(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_correct_vendor_data_inserted_into_cold_store_payload_campaigns(self):

        entity_types = [Entity.Campaign, Entity.AdSet, Entity.Ad]
        fb_model_map = {
            Entity.Campaign: FB_CAMPAIGN_MODEL,
            Entity.AdSet: FB_ADSET_MODEL,
            Entity.Ad: FB_AD_MODEL
        }
        get_all_method_map = {
            Entity.Campaign: 'get_campaigns',
            Entity.AdSet: 'get_ad_sets',
            Entity.Ad: 'get_ads'
        }

        for entity_type in entity_types:

            fbid = random.gen_string_id()
            FB_MODEL = fb_model_map[entity_type]
            get_method_name = get_all_method_map[entity_type]

            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                report_type=ReportType.entity,
                report_variant=entity_type,
                tokens=['blah']
            )

            universal_id_should_be = generate_universal_id(
                ad_account_id=self.ad_account_id,
                report_type=ReportType.entity,
                entity_id=fbid,
                entity_type=entity_type
            )

            fb_data = FB_MODEL(fbid=fbid)
            fb_data[FB_MODEL.Field.account_id] = self.ad_account_id
            entities_data = [fb_data]

            with mock.patch.object(FB_ADACCOUNT_MODEL, get_method_name, return_value=entities_data), \
                 mock.patch.object(ChunkDumpStore, 'store') as store:

                data_received = list(iter_collect_entities_per_adaccount(job_scope, None))

            assert store.called
            aa, kk = store.call_args
            assert not kk
            assert len(aa) == 1
            data_actual = aa[0]

            vendor_data_key = '__oprm'

            assert vendor_data_key in data_actual and type(data_actual[vendor_data_key]) == dict
            assert data_actual[vendor_data_key] == {
                'id': universal_id_should_be
            }
