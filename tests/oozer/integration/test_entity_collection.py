from datetime import datetime

from tests.base.testcase import TestCase, integration

from common.enums.entity import Entity
from config.facebook import TOKEN, AD_ACCOUNT
from oozer.common.job_scope import JobScope
from oozer.common.facebook_api import FacebookApiContext
from oozer.entities.facebook_entity_collector import \
    collect_entities_for_adaccount, _get_entities_for_adaccount


@integration
class TestingEntityCollection(TestCase):

    def test_fetch_all_campaigns(self):

        with FacebookApiContext(TOKEN) as ctx:
            entities = _get_entities_for_adaccount(
                AD_ACCOUNT,
                Entity.Campaign
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_adsets(self):

        with FacebookApiContext(TOKEN) as ctx:
            entities = _get_entities_for_adaccount(
                AD_ACCOUNT,
                Entity.AdSet
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt

    def test_fetch_all_ads(self):

        with FacebookApiContext(TOKEN) as ctx:
            entities = _get_entities_for_adaccount(
                AD_ACCOUNT,
                Entity.Ad
            )
            cnt = 0
            for entity in entities:
                cnt += 1
                break

            assert cnt


class TestingEntityCollectionPipeline(TestCase):

    @integration
    def test_pipeline(self):

        job_scope = JobScope(
            ad_account_id=AD_ACCOUNT,
            tokens=[TOKEN],
            report_time=datetime.utcnow(),
            report_type='entities',
        )

        fb_models = collect_entities_for_adaccount(Entity.Campaign, job_scope)

        cnt = 0
        for fb_model in fb_models:
            cnt += 1
            if cnt == 4:
                break

        assert cnt == 4
