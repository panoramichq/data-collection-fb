# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

import config.application

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id
from sweep_builder.expectation_builder.expectations import iter_expectations
from sweep_builder.data_containers.reality_claim import RealityClaim
from tests.base.random import gen_string_id


class ScopeJobsExpectationsTests(TestCase):
    def test_aa_import_expectation_not_generated_on_nontoken(self):

        reality_claim = RealityClaim(entity_type=Entity.Scope, entity_id=gen_string_id(), tokens=[])

        results = list(iter_expectations([reality_claim]))

        assert not results

    def test_aa_import_expectation_generated_on_token(self):

        entity_id = gen_string_id()

        reality_claim = RealityClaim(entity_type=Entity.Scope, entity_id=entity_id, tokens=['blah'])

        results = list(iter_expectations([reality_claim]))

        assert results
        assert len(results) == 2
        expectation_claim = results[0]

        assert expectation_claim.entity_id == reality_claim.entity_id
        assert expectation_claim.entity_type == reality_claim.entity_type

        assert len(expectation_claim.job_signatures) == 1
        job_signature = expectation_claim.job_signatures[0]

        assert job_signature.job_id == generate_id(
            namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
            entity_type=Entity.Scope,
            entity_id=entity_id,
            report_type=ReportType.import_accounts,
            report_variant=Entity.AdAccount,
        )
