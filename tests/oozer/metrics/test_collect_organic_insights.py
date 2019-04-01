from datetime import datetime

from common.enums.entity import Entity
from oozer.common.enum import ReportEntityApiKind
from oozer.common.job_scope import JobScope
from oozer.metrics import collect_organic_insights
from tests.base.testcase import TestCase
from tests.base import random


class VendorOrganicDataInjectionTests(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.entity_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_detect_report_api_kind_success(self):
        test_cases_map = [
            (Entity.Page, ReportEntityApiKind.Page),
            (Entity.PagePost, ReportEntityApiKind.Post),
            (Entity.PageVideo, ReportEntityApiKind.Video),
        ]

        for entity_type, expected_result in test_cases_map:
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    ad_account_id=self.ad_account_id,
                    entity_type=entity_type,
                    entity_id=self.ad_account_id,
                    tokens=['blah'],
                    report_time=datetime.utcnow(),
                    report_type='entity',
                    sweep_id='1',
                )
                result = collect_organic_insights.InsightsOrganic._detect_report_api_kind(job_scope)
                assert result == expected_result

    def test_detect_report_api_kind_all_entities(self):
        for entity_type in Entity.ALL:
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    ad_account_id=self.ad_account_id,
                    entity_type=entity_type,
                    entity_id=self.ad_account_id,
                    tokens=['blah'],
                    report_time=datetime.utcnow(),
                    report_type='entity',
                    sweep_id='1',
                )
                if entity_type in Entity.AA_SCOPED or entity_type in [Entity.Comment, Entity.Scope]:
                    with self.assertRaises(ValueError):
                        collect_organic_insights.InsightsOrganic._detect_report_api_kind(job_scope)
                else:
                    result = collect_organic_insights.InsightsOrganic._detect_report_api_kind(job_scope)
                    assert result != ''
