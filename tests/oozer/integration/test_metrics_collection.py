from tests.base.testcase import TestCase, integration

from config.facebook import TOKEN, AD_ACCOUNT

from oozer.common.enum import FB_ADACCOUNT_MODEL, FB_AD_MODEL
from oozer.common.facebook_api import FacebookApiContext

from oozer.metrics.facebook_metrics_collector import get_insights

@integration
class TestMetricsCollection(TestCase):

    pass

    # def test_fetch_insights_adaccount_campaigns_lifetime(self):
    #     with FacebookApiContext(TOKEN) as metrics_collector:
    #
    #         metrics = get_insights(
    #             FB_ADACCOUNT_MODEL,
    #             AD_ACCOUNT,
    #             {'date_preset': 'lifetime', 'level': 'campaign'}
    #         )
    #         print(metrics)
    #         assert metrics
    #
    # def test_fetch_insights_adaccount_adsets_lifetime(self):
    #     with FacebookApiContext(TOKEN) as metrics_collector:
    #
    #         metrics = get_insights(
    #             FB_ADACCOUNT_MODEL,
    #             AD_ACCOUNT,
    #             {'date_preset': 'lifetime', 'level': 'adset'}
    #         )
    #
    #         print(metrics)
    #         assert metrics
    #
    #
