from common.enums.entity import Entity
from common.enums.jobtype import JobType, detect_job_type
from common.enums.reporttype import ReportType
from tests.base.testcase import TestCase


class TestJobType(TestCase):
    def test_cover_all_known_entity_job_types(self):
        for entity_type in set.union(Entity.AA_SCOPED, Entity.NON_AA_SCOPED):
            with self.subTest(f'Entity type = "{entity_type}"'):
                assert (
                    detect_job_type(ReportType.entity, entity_type) != JobType.UNKNOWN
                ), f'Entity report for "{entity_type}" should be known job type'

    def test_cover_all_known_lifetime_metrics_job_types(self):
        for entity_type in set.union(Entity.AA_SCOPED, Entity.NON_AA_SCOPED):
            with self.subTest(f'Entity type = "{entity_type}"'):
                assert (
                    detect_job_type(ReportType.lifetime, entity_type) != JobType.UNKNOWN
                ), f'Lifetime report for "{entity_type}" should be known job type'

    def test_cover_all_known_entity_organic_job_types(self):
        for entity_type in Entity.NON_AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                assert (
                    detect_job_type(ReportType.entity, entity_type) == JobType.ORGANIC_DATA
                ), f'Entity report for "{entity_type}" should be organic job type'

    def test_cover_all_known_lifetime_metrics_organic_job_types(self):
        for entity_type in Entity.NON_AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                assert (
                    detect_job_type(ReportType.lifetime, entity_type) == JobType.ORGANIC_DATA
                ), f'Lifetime report for "{entity_type}" should be organic job type'

    def test_cover_all_known_entity_paid_job_types(self):
        for entity_type in Entity.AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                assert (
                    detect_job_type(ReportType.entity, entity_type) == JobType.PAID_DATA
                ), f'Entity report for "{entity_type}" should be paid job type'

    def test_cover_all_known_lifetime_metrics_paid_job_types(self):
        for entity_type in Entity.AA_SCOPED:
            with self.subTest(f'Entity type = "{entity_type}"'):
                assert (
                    detect_job_type(ReportType.lifetime, entity_type) == JobType.PAID_DATA
                ), f'Lifetime report for "{entity_type}" should be paid job type'

    def test_cover_global_job_types(self):
        for report_type in (
            ReportType.sync_expectations,
            ReportType.sync_status,
            ReportType.import_accounts,
            ReportType.import_pages,
        ):
            with self.subTest(f'Report type = "{report_type}"'):
                assert (
                    detect_job_type(report_type) == JobType.GLOBAL
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
                assert (
                    detect_job_type(report_type, report_variant) == JobType.UNKNOWN
                ), f'Report type "{report_type}" and report variant "{report_variant}" should be unknown job type'
