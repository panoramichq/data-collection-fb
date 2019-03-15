# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from oozer.common.job_scope import JobScope
from oozer import inventory
from tests.base.random import gen_string_id


class TestLooperReportTypeInventoryResolution(TestCase):
    """
    Tests that looper code will recognize the types of reports we expect it to
    recognize from the job ID
    """

    def _job_scope_factory(self, **data):

        base_data = dict(
            sweep_id=gen_string_id(),
            ad_account_id=gen_string_id(),
            entity_id=None,
            entity_type=None,
            report_type=None,
            report_variant=None,
            range_start=None,
            range_end=None,
            tokens=None,
            is_derivative=False,
        )
        base_data.update(data)

        assert base_data['report_type'], 'Hey, cmon, must pass that value to us'

        return JobScope(base_data)

    def test_aa_collection_job(self):
        job_scope = self._job_scope_factory(report_type=ReportType.entity, report_variant=Entity.AdAccount)

        assert inventory.resolve_job_scope_to_celery_task(
            job_scope
        ), f"Ad account must have report handler for {ReportType.entity}"

    def test_entity_report_types_entities_per_aa(self):

        entity_types = [
            Entity.Campaign,
            Entity.AdSet,
            Entity.Ad,
            Entity.AdCreative,
            Entity.AdVideo,
            Entity.CustomAudience,
        ]
        report_type = ReportType.entity

        for entity_type in entity_types:
            job_scope = self._job_scope_factory(report_type=report_type, report_variant=entity_type)

            assert inventory.resolve_job_scope_to_celery_task(
                job_scope
            ), f"Entity {entity_type} must have report handler for {report_type}"

    def test_entity_report_types_entities_per_page(self):

        entity_types = [Entity.PagePost]
        report_type = ReportType.entity

        for entity_type in entity_types:
            job_scope = self._job_scope_factory(report_type=report_type, report_variant=entity_type)

            assert inventory.resolve_job_scope_to_celery_task(
                job_scope
            ), f"Entity {entity_type} must have report handler for {report_type}"

    def test_entity_report_types_a_daily_insights(self):

        entity_types = [Entity.Ad]
        report_types = [ReportType.day_age_gender, ReportType.day_dma, ReportType.day_hour, ReportType.day_platform]

        for entity_type in entity_types:

            for report_type in report_types:

                job_scope = self._job_scope_factory(report_type=report_type, report_variant=entity_type)

                assert inventory.resolve_job_scope_to_celery_task(
                    job_scope
                ), f"Entity {entity_type} must have report handler for {report_type}"

    def test_entity_report_types_lifetime_insights(self):

        entity_types = [Entity.Campaign, Entity.AdSet, Entity.Ad]
        report_types = [ReportType.lifetime]

        for entity_type in entity_types:

            for report_type in report_types:

                job_scope = self._job_scope_factory(report_type=report_type, report_variant=entity_type)

                assert inventory.resolve_job_scope_to_celery_task(
                    job_scope
                ), f"Entity {entity_type} must have report handler for {report_type}"

    def test_scope_refresh_job_handlers_registration(self):

        entity_types = [Entity.Scope]
        report_types = [ReportType.import_accounts]

        for entity_type in entity_types:

            for report_type in report_types:

                job_scope = self._job_scope_factory(report_type=report_type, report_variant=entity_type)

                assert inventory.resolve_job_scope_to_celery_task(
                    job_scope
                ), f"Entity {entity_type} must have report handler for {report_type}"

    def test_sync_expectations_job_handler_registration(self):

        entity_types = [Entity.AdAccount]
        report_types = [ReportType.sync_expectations]

        for entity_type in entity_types:

            for report_type in report_types:

                job_scope = self._job_scope_factory(report_type=report_type, report_variant=entity_type)

                assert inventory.resolve_job_scope_to_celery_task(
                    job_scope
                ), f"Entity {entity_type} must have report handler for {report_type}"
