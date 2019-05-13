# must be first, as it does event loop patching and other "first" things
from common.page_tokens import PageTokenManager
from oozer.entities.collect_entities_iterators import (
    iter_collect_entities_per_page,
    iter_collect_entities_per_page_graph,
)
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_universal_id
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.common.job_scope import JobScope
from oozer.common.enum import FB_PAGE_MODEL, FB_PAGE_POST_MODEL, FB_AD_VIDEO_MODEL

from tests.base import random


class TestCollectEntitiesPerPage(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_correct_vendor_data_inserted_into_cold_store_payload_posts(self):

        entity_types = [Entity.PagePost, Entity.PageVideo]
        fb_model_map = {Entity.PagePost: FB_PAGE_POST_MODEL, Entity.PageVideo: FB_AD_VIDEO_MODEL}
        get_all_method_map = {Entity.PagePost: 'get_posts', Entity.PageVideo: 'get_videos'}

        for entity_type in entity_types:
            with self.subTest(f'Entity type = "{entity_type}"'):
                fbid = random.gen_string_id()
                FB_MODEL = fb_model_map[entity_type]
                get_method_name = get_all_method_map[entity_type]

                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                universal_id_should_be = generate_universal_id(
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    entity_id=fbid,
                    entity_type=entity_type,
                )

                fb_data = FB_MODEL(fbid=fbid)
                fb_data['account_id'] = '0'

                entities_data = [fb_data]
                with mock.patch.object(FB_PAGE_MODEL, get_method_name, return_value=entities_data), mock.patch.object(
                    ChunkDumpStore, 'store'
                ) as store:

                    list(iter_collect_entities_per_page(job_scope))

                assert store.called
                store_args, store_keyword_args = store.call_args
                assert not store_keyword_args
                assert len(store_args) == 1, 'Store method should be called with just 1 parameter'

                data_actual = store_args[0]

                vendor_data_key = '__oprm'

                assert (
                    vendor_data_key in data_actual and type(data_actual[vendor_data_key]) == dict
                ), 'Special vendor key is present in the returned data'
                assert data_actual[vendor_data_key] == {
                    'id': universal_id_should_be
                }, 'Vendor data is set with the right universal id'


class TestCollectEntitiesPerPageGraph(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_correct_vendor_data_inserted_into_cold_store_payload_posts(self):

        entity_types = [Entity.PagePostPromotable]
        fb_model_map = {Entity.PagePostPromotable: FB_PAGE_POST_MODEL}

        for entity_type in entity_types:
            with self.subTest(f'Entity type - "{entity_type}"'):
                fbid = random.gen_string_id()
                FB_MODEL = fb_model_map[entity_type]

                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type,
                    tokens=['user-token'],
                )

                universal_id_should_be = generate_universal_id(
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    entity_id=fbid,
                    entity_type=entity_type,
                )

                fb_data = FB_MODEL(fbid=fbid)
                fb_data['account_id'] = '0'

                entities_data = [fb_data]
                with mock.patch.object(
                    PageTokenManager, 'get_best_token', return_value=None
                ) as get_best_token, mock.patch.object(
                    FB_PAGE_MODEL, 'get_feed', return_value=entities_data
                ), mock.patch.object(
                    FB_PAGE_MODEL, 'get_ads_posts', return_value=entities_data
                ), mock.patch.object(
                    ChunkDumpStore, 'store'
                ) as store, mock.patch.object(
                    FB_PAGE_POST_MODEL, 'get', side_effect=lambda field: field == 'is_eligible_for_promotion'
                ):
                    list(iter_collect_entities_per_page_graph(job_scope))

                assert get_best_token.called
                assert store.called

                store_args, store_keyword_args = store.call_args
                assert not store_keyword_args
                assert len(store_args) == 1, 'Store method should be called with just 1 parameter'

                data_actual = store_args[0]

                vendor_data_key = '__oprm'

                assert (
                    vendor_data_key in data_actual and type(data_actual[vendor_data_key]) == dict
                ), 'Special vendor key is present in the returned data'
                assert data_actual[vendor_data_key] == {
                    'id': universal_id_should_be
                }, 'Vendor data is set with the right universal id'
