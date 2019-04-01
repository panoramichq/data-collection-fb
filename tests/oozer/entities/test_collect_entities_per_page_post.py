# must be first, as it does event loop patching and other "first" things
from oozer.entities.collect_entities_iterators import iter_collect_entities_per_page_post
from tests.base.testcase import TestCase, mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_universal_id
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.common.job_scope import JobScope
from oozer.common.enum import FB_COMMENT_MODEL, FB_PAGE_POST_MODEL

from tests.base import random


class TestCollectEntitiesPerPagePost(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_correct_vendor_data_inserted_into_cold_store_payload_comments(self):

        entity_types = [Entity.Comment]
        fb_model_map = {Entity.Comment: FB_COMMENT_MODEL}
        get_all_method_map = {Entity.Comment: 'get_comments'}

        for entity_type in entity_types:

            fbid = random.gen_string_id()
            fb_model_klass = fb_model_map[entity_type]
            get_method_name = get_all_method_map[entity_type]

            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                entity_id=self.ad_account_id,
                report_type=ReportType.entity,
                report_variant=entity_type,
                tokens=['blah'],
            )

            universal_id_should_be = generate_universal_id(
                ad_account_id=self.ad_account_id, report_type=ReportType.entity, entity_id=fbid, entity_type=entity_type
            )

            fb_data = fb_model_klass(fbid=fbid)
            fb_data['account_id'] = '0'

            entities_data = [fb_data]
            with mock.patch.object(FB_PAGE_POST_MODEL, get_method_name, return_value=entities_data), mock.patch.object(
                ChunkDumpStore, 'store'
            ) as store:

                list(iter_collect_entities_per_page_post(job_scope))

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
