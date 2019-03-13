from unittest.mock import patch

from common.enums.entity import Entity
from common.job_signature import JobSignature
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.selector import select_signature


@patch('sweep_builder.selector._fetch_job_report')
@patch('sweep_builder.selector.should_select')
def test_select_signature_default_to_normative(mock_should_select, _):
    expected = JobSignature(job_id='cccc')
    claim = ExpectationClaim(
        entity_type=Entity.Ad,
        entity_id='1111',
        normative_job_signature=expected,
        effective_job_signatures=[
            JobSignature(job_id='aaaa'),
            JobSignature(job_id='bbbb'),
        ]
    )
    mock_should_select.side_effect = [False, False]

    signature, report = select_signature(claim)

    assert signature == expected


@patch('sweep_builder.selector._fetch_job_report')
@patch('sweep_builder.selector.should_select')
def test_select_signature_select_first_true(mock_should_select, _):
    expected = JobSignature(job_id='cccc')
    claim = ExpectationClaim(
        entity_type=Entity.Ad,
        entity_id='1111',
        normative_job_signature=JobSignature(job_id='bbbb'),
        effective_job_signatures=[
            JobSignature(job_id='aaaa'),
            expected,
        ]
    )
    mock_should_select.side_effect = [False, True]

    signature, report = select_signature(claim)

    assert signature == expected
