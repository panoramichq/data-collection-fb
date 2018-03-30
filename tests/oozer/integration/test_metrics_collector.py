from tests.base.testcase import TestCase, integration

from typing import Dict, List, Tuple
from unittest import mock, skip

from config.facebook import TOKEN, AD_ACCOUNT

from common.enums.entity import Entity
from oozer.common.facebook_api import FacebookApiContext
from oozer.common.job_scope import JobScope
from common.enums.reporttype import ReportType
from oozer.common import cold_storage

from oozer.metrics.collect_insights import Insights


@skip
@integration('facebook')
class IntegrationTestingMetricsCollection(TestCase):

    def test_fetch_insights_adaccount_campaigns_lifetime(self):
        with FacebookApiContext(TOKEN) as context:

            entity = context.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            metrics = Insights.iter_insights(
                entity,
                {'date_preset': 'lifetime', 'level': 'campaign'}
            )

            datum = None
            for datum in metrics:
                break

            assert datum

    def test_fetch_insights_adaccount_adsets_lifetime(self):
        with FacebookApiContext(TOKEN) as context:
            entity = context.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            metrics = Insights.iter_insights(
                entity,
                {'date_preset': 'lifetime', 'level': 'adset'}
            )

            datum = None
            for datum in metrics:
                break
            assert datum

    def test_fetch_insights_adaccount_ad_lifetime(self):
        with FacebookApiContext(TOKEN) as context:
            entity = context.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            metrics = Insights.iter_insights(
                entity,
                {'date_preset': 'lifetime', 'level': 'ad'}
            )

            datum = None
            for datum in metrics:
                break
            assert datum

@skip
@integration('facebook')
class IntegrationTestingMetricsCollectionPipeline(TestCase):

    def test_lifetime_campaigns(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            report_type=ReportType.lifetime,
            report_variant=Entity.Campaign,
            sweep_id='sweep',
            tokens=[TOKEN],
        )

        captured_data = []  # type: List[Tuple[Dict, JobScope, int]]
        def _store(data, job_scope, chunk_marker=0):
            captured_data.append((data, job_scope, chunk_marker))

        with mock.patch.object(cold_storage, 'store', _store):
            data_iter = Insights.iter_collect_insights(job_scope, None)
            cnt = 0
            for datum in data_iter:
                cnt += 1
                if cnt == 4:
                    break

        assert cnt == 4

        for datum, job_scope_inner, _ in captured_data:
            assert datum['campaign_id'] == job_scope_inner.entity_id

    def test_hourly_ads_per_parent(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            range_start='2017-12-31',
            ad_account_timezone_name='Europe/London',
            report_type=ReportType.day_hour,
            report_variant=Entity.Ad,
            sweep_id='sweep',
            tokens=[TOKEN],
        )

        data_iter = Insights.iter_collect_insights(job_scope, None)
        cnt = 0
        for datum in data_iter:
            cnt += 1
            if cnt == 4:
                break

    def test_hourly_ad(self):

        job_scope = JobScope(
            # report_variant=Entity.Ad,
            ad_account_id=AD_ACCOUNT,
            entity_id='23842698250300224',
            entity_type=Entity.Ad,
            range_start='2017-12-31',
            report_type=ReportType.day_dma,
            sweep_id='sweep',
            tokens=[TOKEN],
        )

        data_iter = Insights.iter_collect_insights(job_scope, None)
        cnt = 0
        for datum in data_iter:
            cnt += 1
            if cnt == 4:
                break
