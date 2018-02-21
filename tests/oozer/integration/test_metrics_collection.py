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

            i = metrics_collector.get_insights(
                FB_ADACCOUNT_MODEL,
                AD_ACCOUNT,
                {'date_preset': 'lifetime', 'level': 'campaign'}
            )
            # i = metrics_collector.get_insights(FB_ADACCOUNT_MODEL, AD_ACCOUNT, { 'level': 'ad' } )
            # i = metrics_collector.get_insights(FB_AD_MODEL, '23842753449240224', { } )

            # TODO: Maybe hide this into the FacebookAsyncReport class
            while not i.completed():
                i.refresh()
                time.sleep(1)

            result = i.read()
            print(result)
            assert result


