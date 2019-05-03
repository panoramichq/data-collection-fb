from datetime import datetime
from unittest.mock import patch

from common.id_tools import parse_id
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectations_inventory.inventory import entity_expectation_generator_map
from tests.base.testcase import TestCase

from common.enums.entity import Entity
from oozer.common.job_scope import JobScope
from oozer import inventory


class TestLooperReportTypeInventoryResolution(TestCase):

    _metrics_module = 'sweep_builder.expectation_builder.expectations_inventory.metrics'

    @patch(f'{_metrics_module}.breakdowns.iter_reality_per_ad_account_claim')
    @patch(f'{_metrics_module}.lifetime.iter_reality_per_ad_account_claim')
    def test_resolve_job_scope_to_celery_task_ad_account(self, mock_lifetime_iter, mock_breakdowns_iter):
        real_claim = RealityClaim(
            entity_id='A1',
            ad_account_id='A1',
            entity_type=Entity.AdAccount,
            tokens='bogus',
            timezone='America/Los_Angeles',
        )
        mock_lifetime_iter.return_value = []
        mock_breakdowns_iter.return_value = [
            RealityClaim(
                entity_id='AD1',
                ad_account_id='A1',
                entity_type=Entity.Ad,
                tokens='bogus',
                range_start=datetime(2019, 1, 20, 12, 0),
                timezone='America/Los_Angeles',
            )
        ]
        for job_generator in entity_expectation_generator_map[Entity.AdAccount]:
            with self.subTest(job_generator=job_generator):
                exp_claim = next(job_generator(real_claim))

                job_scope = JobScope(parse_id(exp_claim.job_id))

                assert inventory.resolve_job_scope_to_celery_task(job_scope)

    def test_resolve_job_scope_to_celery_task_page_post(self):
        real_claim = RealityClaim(entity_id='PP1', ad_account_id='P1', entity_type=Entity.PagePost, tokens='bogus')
        for job_generator in entity_expectation_generator_map[Entity.PagePost]:
            with self.subTest(job_generator=job_generator):
                exp_claim = next(job_generator(real_claim))

                job_scope = JobScope(parse_id(exp_claim.job_id))

                assert inventory.resolve_job_scope_to_celery_task(job_scope)

    def test_resolve_job_scope_to_celery_task_page_video(self):
        real_claim = RealityClaim(entity_id='PV1', ad_account_id='P1', entity_type=Entity.PageVideo, tokens='bogus')
        for job_generator in entity_expectation_generator_map[Entity.PageVideo]:
            with self.subTest(job_generator=job_generator):
                exp_claim = next(job_generator(real_claim))

                job_scope = JobScope(parse_id(exp_claim.job_id))

                assert inventory.resolve_job_scope_to_celery_task(job_scope)
