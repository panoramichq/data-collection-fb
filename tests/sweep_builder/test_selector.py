from unittest.mock import patch, Mock

import pytest

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.job_signature import JobSignature
from common.store.jobreport import JobReport
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.selector import select_signature, should_select


@patch('sweep_builder.selector._fetch_job_report')
@patch('sweep_builder.selector.should_select')
def test_select_signature_default_to_normative(mock_should_select, _):
    expected = JobSignature(job_id='cccc')
    claim = ExpectationClaim(
        entity_type=Entity.Ad,
        entity_id='1111',
        normative_job_signature=expected,
        effective_job_signatures=[JobSignature(job_id='aaaa'), JobSignature(job_id='bbbb')],
    )
    mock_should_select.side_effect = [False, False]

    scorable_claim = select_signature(claim)

    assert scorable_claim.selected_job_signature == expected


@patch('sweep_builder.selector._fetch_job_report')
@patch('sweep_builder.selector.should_select')
def test_select_signature_select_first_true(mock_should_select, _):
    expected = JobSignature(job_id='cccc')
    claim = ExpectationClaim(
        entity_type=Entity.Ad,
        entity_id='1111',
        normative_job_signature=JobSignature(job_id='bbbb'),
        effective_job_signatures=[JobSignature(job_id='aaaa'), expected],
    )
    mock_should_select.side_effect = [False, True]

    scorable_claim = select_signature(claim)

    assert scorable_claim.selected_job_signature == expected


def test_should_select_report_is_none():
    assert should_select(None)


@pytest.mark.parametrize('bucket', [
    FailureBucket.WorkingOnIt,
    FailureBucket.Success,
    FailureBucket.Other,
    FailureBucket.Throttling,
])
def test_should_select_last_error_not_too_large(bucket):
    report = JobReport(last_failure_bucket=bucket)

    assert should_select(report)


def test_should_select_last_error_is_too_large_none_fails_in_row():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=None)

    assert should_select(report)


@patch('sweep_builder.selector.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_should_select_last_error_is_too_large_none_fails_under_limit():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=1)

    assert should_select(report)


@patch('sweep_builder.selector.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_should_select_last_error_is_too_large_fails_in_row_over_limit():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=3)

    assert not should_select(report)
