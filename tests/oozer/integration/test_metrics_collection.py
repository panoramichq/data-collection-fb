from tests.base.testcase import TestCase, integration
from datetime import datetime

from config.facebook import TOKEN, AD_ACCOUNT

from common.enums.entity import Entity
from oozer.common.facebook_api import FacebookApiContext
from oozer.common.job_scope import JobScope
from oozer.common.job_context import JobContext
from oozer.common.facebook_async_report import FacebookReportDefinition

from oozer.metrics.facebook_metrics_collector import _execute_report, \
    collect_insights



@integration
class TestMetricsCollection(TestCase):

    def test_fetch_insights_adaccount_campaigns_lifetime(self):
        with FacebookApiContext(TOKEN) as context:

            entity = context.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            metrics = _execute_report(
                entity,
                {'date_preset': 'lifetime', 'level': 'campaign'},
                JobScope()
            )

            assert metrics

    def test_fetch_insights_adaccount_adsets_lifetime(self):
        with FacebookApiContext(TOKEN) as context:
            entity = context.to_fb_model(AD_ACCOUNT, Entity.AdAccount)

            metrics = _execute_report(
                entity,
                {'date_preset': 'lifetime', 'level': 'adset'},
                JobScope()
            )

            print(metrics)
            assert metrics


class TestingMetricsCollectionPipeline(TestCase):

    @integration
    def test_pipeline(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entities',
            sweep_id='1',
            entity_id=AD_ACCOUNT
        )

        report_params = FacebookReportDefinition(
            level='campaign',
            date_preset='lifetime'
        )

        fb_models = collect_insights(
            Entity.AdAccount, report_params, job_scope, JobContext()
        )

        cnt = 0
        for fb_model in fb_models:
            cnt += 1
            if cnt == 4:
                break

        assert cnt == 4
