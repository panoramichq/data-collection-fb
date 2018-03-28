# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, mock

import functools

from common import id_tools
from common.facebook.enums.entity import Entity
from common.facebook.enums.reporttype import ReportType
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.metrics import collect_insights, vendor_data_extractor
from oozer.common.job_scope import JobScope
from tests.base import random


NS = id_tools.NAMESPACE
D = id_tools.ID_DELIMITER


class VendorDataUniversalIdExtraction(TestCase):

    def test_entity_level_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            vendor_data = vendor_data_extractor._from_non_segmented_entity(
                {
                    entity_id_attr_name_map[entity_type]: 'SomeID'
                },
                # used by code and for ID
                entity_type=entity_type,
                # data used for ID
                ad_account_id='AAID',
                report_type='reporttype',
                # range_start=None,
            )

            universal_id_should_be = D.join([
                'oprm',
                'm',
                NS,
                'AAID',
                entity_type, # entity Type
                'SomeID', # entity ID
                'reporttype',
                # '', # report variant
                # '', # Range start
                # '', # Range end
            ])

            assert vendor_data == dict(
                id=universal_id_should_be,
                entity_type=entity_type,
                entity_id='SomeID'
            )

    def test_hour_level_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            fn = functools.partial(
                vendor_data_extractor._from_hour_segmented_entity,
                # in summer London is 1 hour ahead of UTC.
                # so it's useful for testing easy offset from UTC
                'Europe/London'
            )

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "ctr": "0",
                "date_start": "2018-06-02",  # making sure this is summer so it's +1 hour vs UTC
                "date_stop": "2018-06-02",
                "frequency": "0",
                "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
                "impressions": "371",
            }

            vendor_data = fn(
                input_data,
                # used by code and for ID
                entity_type=entity_type,
                # data used for ID
                ad_account_id='AAID',
                report_type='reporttype',
                # range_start=None,
            )

            universal_id_should_be = D.join([
                'oprm',
                'm',
                NS,
                'AAID',
                entity_type, # entity Type
                'SomeID', # entity ID
                'reporttype',
                '', # report variant
                '2018-06-01T23', # Range start. Note that it's one hour away from input! It's UTC
                # '', # Range end
            ])

            assert vendor_data == dict(
                id=universal_id_should_be,
                # Note that it's UTC-based, not AdAccount timezone based
                # and does NOT match the date, time in the original payload
                #  2018-06-02T00:00:00
                range_start='2018-06-01T23:00:00',
                entity_type=entity_type,
                entity_id='SomeID'
            )

    def test_agegender_level_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "date_start": "2018-06-02",
                "date_stop": "2018-06-02",
                "age": "18-24",  # <----------
                "clicks": "10",
                "cpc": "0.117",
                "cpm": "2.521552",
                "cpp": "2.526998",
                "ctr": "2.155172",
                "gender": "female",  # <----------
                "impressions": "464",
            }

            vendor_data = vendor_data_extractor._from_age_gender_segmented_entity(
                input_data,
                # used by code and for ID
                entity_type=entity_type,
                # data used for ID
                ad_account_id='AAID',
                report_type='reporttype',
                # range_start=None,
            )

            universal_id_should_be = D.join([
                'oprm',
                'm',
                NS,
                'AAID',
                entity_type, # entity Type
                'SomeID', # entity ID
                'reporttype',
                '', # report variant
                '2018-06-02', # Range start.
                '', # Range end
                #### extra stuff special to age,gender
                "18-24",  # age
                "female",  # gender
            ])

            assert vendor_data == dict(
                id=universal_id_should_be,
                range_start='2018-06-02',
                entity_type=entity_type,
                entity_id='SomeID'
            )

    def test_platform_level_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "date_start": "2018-06-02",
                "date_stop": "2018-06-02",
                "ctr": "1.88383",
                "frequency": "1.001572",
                "impressions": "637",
                "platform_position": "feed",  # <-----------
                "publisher_platform": "facebook",  # <-----------
                "reach": "636",
            }

            vendor_data = vendor_data_extractor._from_platform_segmented_entity(
                input_data,
                # used by code and for ID
                entity_type=entity_type,
                # data used for ID
                ad_account_id='AAID',
                report_type='reporttype',
                # range_start=None,
            )

            universal_id_should_be = D.join([
                'oprm',
                'm',
                NS,
                'AAID',
                entity_type, # entity Type
                'SomeID', # entity ID
                'reporttype',
                '', # report variant
                '2018-06-02', # Range start.
                '', # Range end
                #### extra stuff special to age,gender
                "facebook",  # publisher_platform
                "feed",  # position
            ])

            assert vendor_data == dict(
                id=universal_id_should_be,
                range_start='2018-06-02',
                entity_type=entity_type,
                entity_id='SomeID'
            )


    def test_dma_level_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "clicks": "0",
                "cpc": "0",
                "cpm": "5",
                "ctr": "0",
                "date_start": "2018-06-02",
                "date_stop": "2018-06-02",
                "dma": "Some Long-running Value (spaces+parens)",  # <-------
                "impressions": "2",
                "reach": "2",
                "spend": "0.01"
            }

            vendor_data = vendor_data_extractor._from_dma_segmented_entity(
                input_data,
                # used by code and for ID
                entity_type=entity_type,
                # data used for ID
                ad_account_id='AAID',
                report_type='reporttype',
                # range_start=None,
            )

            universal_id_should_be = D.join([
                'oprm',
                'm',
                NS,
                'AAID',
                entity_type, # entity Type
                'SomeID', # entity ID
                'reporttype',
                '', # report variant
                '2018-06-02', # Range start.
                '', # Range end
                #### extra stuff special to DMA
                "Some+Long-running+Value+%28spaces%2Bparens%29",
            ])

            assert vendor_data == dict(
                id=universal_id_should_be,
                range_start='2018-06-02',
                entity_type=entity_type,
                entity_id='SomeID'
            )


class VendorDataInjectionTests(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_entity_level_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID'
            }


            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                report_type=ReportType.lifetime,
                report_variant=entity_type,
                tokens=['blah']
            )

            with mock.patch.object(collect_insights.Insights, 'iter_insights', return_value=[input_data]), \
                mock.patch.object(ChunkDumpStore, 'store') as store:

                gg = collect_insights.Insights.iter_collect_insights(
                    job_scope,
                    None
                )
                cnt = 0
                for datum in gg:
                    cnt += 1
                assert cnt == 1

            assert store.called
            aa, kk = store.call_args
            assert not kk
            assert aa == (
                {
                    entity_id_attr_name_map[entity_type]: 'SomeID',
                    '__oprm': {
                        'id': f'oprm|m|fb|{self.ad_account_id}|{entity_type}|SomeID|lifetime',
                        'entity_id': 'SomeID',
                        'entity_type': entity_type
                    }
                },
            )

    def test_hour_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "ctr": "0",
                "date_start": "2018-06-02",  # making sure this is summer so it's +1 hour vs UTC
                "date_stop": "2018-06-02",
                "frequency": "0",
                "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
                "impressions": "371",
            }

            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                ad_account_timezone_name='Europe/London',
                # note that range start is in AdAccount's timezone
                # while we report in UTC time, so reported hour
                # slices may be outside of AA reporting date.
                range_start="2018-06-02",
                report_type=ReportType.day_hour,
                report_variant=entity_type,
                tokens=['blah']
            )

            with mock.patch.object(collect_insights.Insights, 'iter_insights', return_value=[input_data]), \
                 mock.patch.object(ChunkDumpStore, 'store') as store:

                gg = collect_insights.Insights.iter_collect_insights(
                    job_scope,
                    None
                )
                cnt = 0
                for datum in gg:
                    cnt += 1
                assert cnt == 1

            assert store.called
            aa, kk = store.call_args
            assert not kk
            assert aa == (
                {
                    entity_id_attr_name_map[entity_type]: 'SomeID',
                    '__oprm': {
                        'id': f'oprm|m|fb|{self.ad_account_id}|{entity_type}|SomeID|dayhour||2018-06-01T23',
                        'range_start': '2018-06-01T23:00:00', # note it's UTC
                        'entity_id': 'SomeID',
                        'entity_type': entity_type
                    },
                    "ctr": "0",
                    "date_start": "2018-06-02",  # making sure this is summer so it's +1 hour vs UTC
                    "date_stop": "2018-06-02",
                    "frequency": "0",
                    "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
                    "impressions": "371",
                },
            )

    def test_agegender_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "date_start": "2018-06-02",
                "date_stop": "2018-06-02",
                "age": "18-24",  # <----------
                "clicks": "10",
                "cpc": "0.117",
                "cpm": "2.521552",
                "cpp": "2.526998",
                "ctr": "2.155172",
                "gender": "female",  # <----------
                "impressions": "464",
            }

            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                ad_account_timezone_name='Europe/London',
                # note that range start is in AdAccount's timezone
                # while we report in UTC time, so reported hour
                # slices may be outside of AA reporting date.
                range_start="2018-06-02",
                report_type=ReportType.day_age_gender,
                report_variant=entity_type,
                tokens=['blah']
            )

            with mock.patch.object(collect_insights.Insights, 'iter_insights', return_value=[input_data]), \
                 mock.patch.object(ChunkDumpStore, 'store') as store:

                gg = collect_insights.Insights.iter_collect_insights(
                    job_scope,
                    None
                )
                cnt = 0
                for datum in gg:
                    cnt += 1
                assert cnt == 1

            assert store.called
            aa, kk = store.call_args
            assert not kk
            assert aa == (
                {
                    entity_id_attr_name_map[entity_type]: 'SomeID',
                    '__oprm': {
                        'id': f'oprm|m|fb|{self.ad_account_id}|{entity_type}|SomeID|dayagegender||2018-06-02||18-24|female',
                        'range_start': '2018-06-02', # note it's In AA time zone
                        'entity_id': 'SomeID',
                        'entity_type': entity_type
                    },
                    "date_start": "2018-06-02",
                    "date_stop": "2018-06-02",
                    "age": "18-24",  # <----------
                    "clicks": "10",
                    "cpc": "0.117",
                    "cpm": "2.521552",
                    "cpp": "2.526998",
                    "ctr": "2.155172",
                    "gender": "female",  # <----------
                    "impressions": "464",
                },
            )

    def test_dma_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "ctr": "0",
                "clicks": "0",
                "cpc": "0",
                "cpm": "5",
                "date_start": "2018-06-02",
                "date_stop": "2018-06-02",
                "dma": "Some Long-running Value (spaces+parens)",  # <-------
                "impressions": "2",
                "reach": "2",
                "spend": "0.01"
            }

            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                ad_account_timezone_name='Europe/London',
                # note that range start is in AdAccount's timezone
                # while we report in UTC time, so reported hour
                # slices may be outside of AA reporting date.
                range_start="2018-06-02",
                report_type=ReportType.day_dma,
                report_variant=entity_type,
                tokens=['blah']
            )

            with mock.patch.object(collect_insights.Insights, 'iter_insights', return_value=[input_data]), \
                 mock.patch.object(ChunkDumpStore, 'store') as store:

                gg = collect_insights.Insights.iter_collect_insights(
                    job_scope,
                    None
                )
                cnt = 0
                for datum in gg:
                    cnt += 1
                assert cnt == 1

            assert store.called
            aa, kk = store.call_args
            assert not kk
            assert aa == (
                {
                    entity_id_attr_name_map[entity_type]: 'SomeID',
                    '__oprm': {
                        'id': f'oprm|m|fb|{self.ad_account_id}|{entity_type}|SomeID|daydma||2018-06-02||Some+Long-running+Value+%28spaces%2Bparens%29',
                        'range_start': '2018-06-02', # note it's In AA time zone
                        'entity_id': 'SomeID',
                        'entity_type': entity_type
                    },
                    "clicks": "0",
                    "cpc": "0",
                    "cpm": "5",
                    "ctr": "0",
                    "date_start": "2018-06-02",
                    "date_stop": "2018-06-02",
                    "dma": "Some Long-running Value (spaces+parens)",  # <-------
                    "impressions": "2",
                    "reach": "2",
                    "spend": "0.01"
                },
            )

    def test_platform_data(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad
        ]

        # intentionally NOT reusing collect_insights._entity_type_id_field_map map
        # effectively, here we are testing it too.
        entity_id_attr_name_map = {
            Entity.Campaign: 'campaign_id',
            Entity.AdSet: 'adset_id',
            Entity.Ad: 'ad_id'
        }

        for entity_type in entity_types:

            input_data = {
                entity_id_attr_name_map[entity_type]: 'SomeID',
                "date_start": "2018-06-02",
                "date_stop": "2018-06-02",
                "ctr": "1.88383",
                "frequency": "1.001572",
                "impressions": "637",
                "platform_position": "feed",  # <-----------
                "publisher_platform": "facebook",  # <-----------
                "reach": "636",
            }

            job_scope = JobScope(
                sweep_id=self.sweep_id,
                ad_account_id=self.ad_account_id,
                ad_account_timezone_name='Europe/London',
                # note that range start is in AdAccount's timezone
                # while we report in UTC time, so reported hour
                # slices may be outside of AA reporting date.
                range_start="2018-06-02",
                report_type=ReportType.day_platform,
                report_variant=entity_type,
                tokens=['blah']
            )

            with mock.patch.object(collect_insights.Insights, 'iter_insights', return_value=[input_data]), \
                 mock.patch.object(ChunkDumpStore, 'store') as store:

                gg = collect_insights.Insights.iter_collect_insights(
                    job_scope,
                    None
                )
                cnt = 0
                for datum in gg:
                    cnt += 1
                assert cnt == 1

            assert store.called
            aa, kk = store.call_args
            assert not kk
            assert aa == (
                {
                    entity_id_attr_name_map[entity_type]: 'SomeID',
                    '__oprm': {
                        'id': f'oprm|m|fb|{self.ad_account_id}|{entity_type}|SomeID|dayplatform||2018-06-02||facebook|feed',
                        'range_start': '2018-06-02', # note it's In AA time zone
                        'entity_id': 'SomeID',
                        'entity_type': entity_type
                    },
                    "date_start": "2018-06-02",
                    "date_stop": "2018-06-02",
                    "ctr": "1.88383",
                    "frequency": "1.001572",
                    "impressions": "637",
                    "platform_position": "feed",  # <-----------
                    "publisher_platform": "facebook",  # <-----------
                    "reach": "636",
                },
            )
