# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase, skip, mock

import config.application

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id, parse_id_parts
from sweep_builder.expectation_builder.expectations import iter_expectations
from sweep_builder.data_containers.reality_claim import RealityClaim
from tests.base.random import gen_string_id


class AdAccountJobsExpectationsTests(TestCase):

    def test_aa_collection_expectation(self):

        ad_account_id = gen_string_id()

        reality_claim = RealityClaim(
            ad_account_id=ad_account_id,
            entity_id=ad_account_id,
            entity_type=Entity.AdAccount,
            tokens=['blah']
        )


        def is_adaccount_entity_job(expectation_claim):
            first_job_sign = expectation_claim.job_signatures[0].job_id
            parsed_id_parts = parse_id_parts(first_job_sign)
            return parsed_id_parts.report_type == ReportType.entity and parsed_id_parts.report_variant == Entity.AdAccount

        adaccount_entity_expectations = list(filter(is_adaccount_entity_job, iter_expectations([reality_claim])))

        assert adaccount_entity_expectations
        assert len(adaccount_entity_expectations) == 1

        expectation_claim = adaccount_entity_expectations[0]

        assert expectation_claim.entity_id == reality_claim.ad_account_id
        assert expectation_claim.entity_type == reality_claim.entity_type

        assert len(expectation_claim.job_signatures) == 1
        job_signature = expectation_claim.job_signatures[0]

        assert job_signature.job_id == generate_id(
            ad_account_id=ad_account_id,
            entity_id=ad_account_id,
            report_type=ReportType.entity,
            report_variant=Entity.AdAccount
        )
