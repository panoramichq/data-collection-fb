from unittest.mock import patch, sentinel, Mock

import config.application

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id, parse_id_parts
from sweep_builder.expectation_builder.expectations import iter_expectations
from sweep_builder.data_containers.reality_claim import RealityClaim
from tests.base.random import gen_string_id


@patch("sweep_builder.expectation_builder.expectations.entity_expectation_generator_map")
def test_iter_expectations_generates_jobs_from_map(mock_map):
    mock_map.get.return_value = [Mock(return_value=[sentinel.expectation_claim])]

    reality_claim = RealityClaim(entity_id=gen_string_id(), entity_type=Entity.AdAccount)

    assert [sentinel.expectation_claim] == list(iter_expectations([reality_claim]))


def test_aa_import_expectation_not_generated_on_nontoken():
    reality_claim = RealityClaim(entity_type=Entity.Scope, entity_id=gen_string_id(), tokens=[])

    results = list(iter_expectations([reality_claim]))

    assert not results


def test_aa_import_expectation_generated_on_token():
    entity_id = gen_string_id()

    reality_claim = RealityClaim(entity_type=Entity.Scope, entity_id=entity_id, tokens=["blah"])

    results = list(iter_expectations([reality_claim]))

    assert len(results) == 1
    expectation_claim = results[0]

    assert expectation_claim.entity_id == reality_claim.entity_id
    assert expectation_claim.entity_type == reality_claim.entity_type

    assert expectation_claim.normative_job_id == generate_id(
        namespace=config.application.UNIVERSAL_ID_SYSTEM_NAMESPACE,
        entity_type=Entity.Scope,
        entity_id=entity_id,
        report_type=ReportType.import_accounts,
        report_variant=Entity.AdAccount,
    )


def test_aa_collection_expectation():
    ad_account_id = gen_string_id()

    reality_claim = RealityClaim(
        ad_account_id=ad_account_id, entity_id=ad_account_id, entity_type=Entity.AdAccount, tokens=["blah"]
    )

    def is_adaccount_entity_job(expectation_claim):
        parsed_id_parts = parse_id_parts(expectation_claim.normative_job_id)
        return parsed_id_parts.report_type == ReportType.entity and parsed_id_parts.report_variant == Entity.AdAccount

    adaccount_entity_expectations = list(filter(is_adaccount_entity_job, iter_expectations([reality_claim])))

    assert len(adaccount_entity_expectations) == 1
    expectation_claim = adaccount_entity_expectations[0]

    assert expectation_claim.entity_id == reality_claim.ad_account_id
    assert expectation_claim.entity_type == reality_claim.entity_type

    assert expectation_claim.normative_job_id == generate_id(
        ad_account_id=ad_account_id,
        entity_id=ad_account_id,
        report_type=ReportType.entity,
        report_variant=Entity.AdAccount,
    )
