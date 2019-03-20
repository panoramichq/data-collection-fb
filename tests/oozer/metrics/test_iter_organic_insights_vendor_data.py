# must be first, as it does event loop patching and other "first" things
from oozer.metrics.vendor_data_extractor import ORGANIC_DATA_ENTITY_ID_MAP
from tests.base.testcase import TestCase, mock

from common import id_tools
from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from oozer.common.cold_storage.batch_store import NormalStore
from oozer.metrics import vendor_data_extractor, collect_organic_insights
from oozer.common.job_scope import JobScope
from tests.base import random

NS = id_tools.NAMESPACE_RAW
D = id_tools.ID_DELIMITER


class VendorOrganicDataUniversalIdExtraction(TestCase):
    def test_entity_level_data(self):

        entity_types = [Entity.PageVideo, Entity.PagePost, Entity.Page]

        for entity_type in entity_types:

            vendor_data = vendor_data_extractor._from_non_segmented_raw_entity(
                {ORGANIC_DATA_ENTITY_ID_MAP[entity_type]: 'SomeID'},
                # used by code and for ID
                entity_type=entity_type,
                # data used for ID
                ad_account_id='AAID',
                report_type='reporttype',
                # range_start=None,
            )

            universal_id_should_be = D.join(
                [
                    'oprm',
                    'm',
                    NS,
                    'AAID',
                    entity_type,  # entity Type
                    'SomeID',  # entity ID
                    'reporttype',
                    # '', # report variant
                    # '', # Range start
                    # '', # Range end
                ]
            )

            assert vendor_data == dict(id=universal_id_should_be, entity_type=entity_type, entity_id='SomeID')


class VendorOrganicDataInjectionTests(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.entity_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_page_video_data(self):

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        input_data = [
            {
                "id": "252165875473429/video_insights/total_video_views/lifetime",
                "name": "total_video_views",
                "values": [{"value": 1504}],
            },
            {
                "id": "252165875473429/video_insights/total_video_views_unique/lifetime",
                "name": "total_video_views_unique",
                "values": [{"value": 1456}],
            },
        ]

        job_scope = JobScope(
            sweep_id=self.sweep_id,
            ad_account_id=self.ad_account_id,
            entity_type=Entity.PageVideo,
            entity_id=self.entity_id,
            report_type=ReportType.lifetime,
            report_variant=Entity.PageVideo,
            tokens=['blah'],
        )

        with mock.patch.object(
            collect_organic_insights.InsightsOrganic, 'iter_video_insights', return_value=input_data
        ), mock.patch.object(NormalStore, 'store') as store:

            gg = collect_organic_insights.InsightsOrganic.iter_collect_insights(job_scope)
            cnt = 0
            for _ in gg:
                cnt += 1
            assert cnt == 1

        assert store.called
        assert len(store.call_args_list) == 2
        sig1, sig2 = store.call_args_list

        aa, kk = sig1
        assert not kk
        assert aa == (
            {
                'page_id': self.ad_account_id,
                'page_video_id': self.entity_id,
                'payload': input_data,
                '__oprm': {
                    'id': f'oprm|m|fb-raw|{self.ad_account_id}|{Entity.PageVideo}|{self.entity_id}|lifetime',
                    'entity_id': self.entity_id,
                    'entity_type': Entity.PageVideo,
                },
            },
        )

        aa, kk = sig2
        assert not kk
        assert aa == (
            {
                'page_id': self.ad_account_id,
                'page_video_id': self.entity_id,
                'total_video_views': 1504,
                'total_video_views_unique': 1456,
                '__oprm': {
                    'id': f'oprm|m|fb|{self.ad_account_id}|{Entity.PageVideo}|{self.entity_id}|lifetime',
                    'entity_id': self.entity_id,
                    'entity_type': Entity.PageVideo,
                },
            },
        )

    def test_other_insights_data(self):

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        input_data = [
            {
                "id": "252165875473429/video_insights/total_video_views/lifetime",
                "name": "total_video_views",
                "values": [{"value": 1504}],
            },
            {
                "id": "252165875473429/video_insights/total_video_views_unique/lifetime",
                "name": "total_video_views_unique",
                "values": [{"value": 1456}],
            },
        ]

        entity_types = {Entity.Page, Entity.PagePost}
        for entity_type in entity_types:
            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                entity_type=entity_type,
                entity_id=self.entity_id,
                report_type=ReportType.lifetime,
                report_variant=Entity.PageVideo,
                tokens=['blah'],
            )

            with mock.patch.object(
                collect_organic_insights.InsightsOrganic, 'iter_other_insights', return_value=input_data
            ), mock.patch.object(NormalStore, 'store') as store:

                gg = collect_organic_insights.InsightsOrganic.iter_collect_insights(job_scope)
                cnt = 0
                for _ in gg:
                    cnt += 1
                assert cnt == 1

            assert store.called
            assert len(store.call_args_list) == 2
            sig1, sig2 = store.call_args_list

            aa, kk = sig1
            assert not kk
            assert aa == (
                {
                    'page_id': self.ad_account_id,
                    ORGANIC_DATA_ENTITY_ID_MAP[entity_type]: self.entity_id,
                    'payload': input_data,
                    '__oprm': {
                        'id': f'oprm|m|fb-raw|{self.ad_account_id}|{entity_type}|{self.entity_id}|lifetime',
                        'entity_id': self.entity_id,
                        'entity_type': entity_type,
                    },
                },
            )

            aa, kk = sig2
            assert not kk
            assert aa == (
                {
                    'page_id': self.ad_account_id,
                    ORGANIC_DATA_ENTITY_ID_MAP[entity_type]: self.entity_id,
                    'total_video_views': 1504,
                    'total_video_views_unique': 1456,
                    '__oprm': {
                        'id': f'oprm|m|fb|{self.ad_account_id}|{entity_type}|{self.entity_id}|lifetime',
                        'entity_id': self.entity_id,
                        'entity_type': entity_type,
                    },
                },
            )
