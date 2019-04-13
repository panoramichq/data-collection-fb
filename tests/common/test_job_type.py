from common.enums.entity import Entity
from common.enums.jobtype import JobType
from common.enums.reporttype import ReportType
from oozer.common.job_scope import JobScope
from tests.base import random
from tests.base.testcase import TestCase


class TestJobScope(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = random.gen_string_id()
        self.scope_id = random.gen_string_id()
        self.ad_account_id = random.gen_string_id()

    def test_cover_all_known_entity_job_types(self):
        for entity_type in set.union(Entity.AA_SCOPED, Entity.NON_AA_SCOPED):
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type != JobType.UNKNOWN
                ), f'Entity report for "{entity_type}" should be known job type'

    def test_cover_all_known_lifetime_metrics_job_types(self):
        for entity_type in set.union(Entity.AA_SCOPED, Entity.NON_AA_SCOPED):
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.lifetime,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type != JobType.UNKNOWN
                ), f'Lifetime report for "{entity_type}" should be known job type'

    def test_cover_all_known_entity_organic_job_types(self):
        for entity_type in Entity.NON_AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type == JobType.ORGANIC_DATA
                ), f'Entity report for "{entity_type}" should be organic job type'

    def test_cover_all_known_lifetime_metrics_organic_job_types(self):
        for entity_type in Entity.NON_AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.lifetime,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type == JobType.ORGANIC_DATA
                ), f'Lifetime report for "{entity_type}" should be organic job type'

    def test_cover_all_known_entity_paid_job_types(self):
        for entity_type in Entity.AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type == JobType.PAID_DATA
                ), f'Entity report for "{entity_type}" should be paid job type'

    def test_cover_all_known_lifetime_metrics_paid_job_types(self):
        for entity_type in Entity.AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=ReportType.lifetime,
                    report_variant=entity_type,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type == JobType.PAID_DATA
                ), f'Lifetime report for "{entity_type}" should be paid job type'

    def test_cover_global_job_types(self):
        for report_type in (
            ReportType.sync_expectations,
            ReportType.sync_status,
            ReportType.import_accounts,
            ReportType.import_pages,
        ):
            with self.subTest(f'Report type = "{report_type}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id, ad_account_id=self.ad_account_id, report_type=report_type, tokens=['blah']
                )

                assert (
                    job_scope.job_type == JobType.GLOBAL
                ), f'Report type "{report_type}" should be known global job type'

    def test_cover_unknown_job_types(self):
        unknown_report_type = 'dummy-type'
        unknown_report_variant = 'dummy-variant'
        test_cases = [
            (unknown_report_type, None),
            (None, unknown_report_variant),
            (unknown_report_type, unknown_report_variant),
            (None, None),
        ]
        for report_type, report_variant in test_cases:
            with self.subTest(f'Report type = "{report_type}"; Report variant = "{report_variant}"'):
                job_scope = JobScope(
                    sweep_id=self.sweep_id,
                    ad_account_id=self.ad_account_id,
                    report_type=report_type,
                    report_variant=report_variant,
                    tokens=['blah'],
                )

                assert (
                    job_scope.job_type == JobType.UNKNOWN
                ), f'Report type "{report_type}" and report variant "{report_variant}" should be unknown job type'
