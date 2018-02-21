from datetime import datetime
import time

from tests.base.testcase import TestCase, integration

from common.enums.entity import Entity
from config.facebook import TOKEN, AD_ACCOUNT
from oozer.common.job_scope import JobScope

from oozer.common.enum import FB_ADACCOUNT_MODEL, FB_AD_MODEL

from oozer.metrics.facebook_metrics_collector import FacebookMetricsCollector

@integration
class TestMetricsCollection(TestCase):

    def test_fetch_insights_adaccount_campaigns_lifetime(self):
        with FacebookMetricsCollector(TOKEN) as metrics_collector:

            metrics = metrics_collector.get_insights(
                FB_ADACCOUNT_MODEL,
                AD_ACCOUNT,
                {'date_preset': 'lifetime', 'level': 'campaign'}
            )
            print(metrics)
            assert metrics

    def test_fetch_insights_adaccount_adsets_lifetime(self):
        with FacebookMetricsCollector(TOKEN) as metrics_collector:

            metrics = metrics_collector.get_insights(
                FB_ADACCOUNT_MODEL,
                AD_ACCOUNT,
                {'date_preset': 'lifetime', 'level': 'adset'}
            )

            print(metrics)
            assert metrics


