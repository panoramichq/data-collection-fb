from datetime import date
from unittest.mock import patch, Mock, sentinel

import pytest

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.job_signature import JobSignature
from common.store.jobreport import JobReport
from sweep_builder.data_containers.entity_node import EntityNode
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.scorable import prefer_job_breakdown, generate_scorable, generate_child_claims


@pytest.yield_fixture(autouse=True)
def patch_config_enabled_flag():
    with patch('sweep_builder.scorable.TASK_BREAKDOWN_ENABLED', True) as _:
        yield


def test_generate_child_claims():
    entity_hierarchy = EntityNode('adset-id', Entity.AdSet)
    ad1, ad2 = EntityNode('ad-id1', Entity.Ad), EntityNode('ad-id2', Entity.Ad)
    entity_hierarchy.add_node(ad1)
    entity_hierarchy.add_node(ad2)
    claim = ExpectationClaim(
        'adset-id',
        Entity.AdSet,
        ReportType.lifetime,
        Entity.Ad,
        JobSignature('fb|ad-account-id|A|adset-id|lifetime|A|2019-02-20'),
        ad_account_id='ad-account-id',
        timezone='timezone',
        entity_hierarchy=entity_hierarchy,
        range_start=date(2019, 2, 20),
    )

    result = list(generate_child_claims(claim))

    assert result == [
        ExpectationClaim(
            'ad-id1',
            Entity.Ad,
            ReportType.lifetime,
            Entity.Ad,
            JobSignature('fb|ad-account-id|A|ad-id1|lifetime|A|2019-02-20'),
            ad_account_id='ad-account-id',
            timezone='timezone',
            entity_hierarchy=ad1,
            range_start=date(2019, 2, 20),
        ),
        ExpectationClaim(
            'ad-id2',
            Entity.Ad,
            ReportType.lifetime,
            Entity.Ad,
            JobSignature('fb|ad-account-id|A|ad-id2|lifetime|A|2019-02-20'),
            ad_account_id='ad-account-id',
            timezone='timezone',
            entity_hierarchy=ad2,
            range_start=date(2019, 2, 20),
        ),
    ]


@patch('sweep_builder.scorable._fetch_job_report')
@patch('sweep_builder.scorable.prefer_job_breakdown')
@patch('sweep_builder.scorable.generate_child_claims')
def test_generate_scorable_claim_not_divisible(mock_generate_child_claims, mock_prefer_job_breakdown, mock_fetch_job_report):
    mock_fetch_job_report.return_value = None
    claim = Mock(
        entity_id='entity_id',
        entity_type=Entity.Ad,
        report_type=ReportType.lifetime,
        job_signature=JobSignature('job_id'),
        is_divisible=False,
    )

    result = list(generate_scorable(claim))

    assert len(result) == 1
    assert not mock_prefer_job_breakdown.called
    assert not mock_generate_child_claims.called


@patch('sweep_builder.scorable._fetch_job_report')
@patch('sweep_builder.scorable.prefer_job_breakdown')
@patch('sweep_builder.scorable.generate_child_claims')
def test_generate_scorable_job_report_none(mock_generate_child_claims, mock_prefer_job_breakdown, mock_fetch_job_report):
    mock_fetch_job_report.return_value = None
    claim = Mock(
        entity_id='entity_id',
        entity_type=Entity.Ad,
        report_type=ReportType.lifetime,
        job_signature=JobSignature('job_id'),
        is_divisible=True,
    )

    result = list(generate_scorable(claim))

    assert len(result) == 1
    assert not mock_prefer_job_breakdown.called
    assert not mock_generate_child_claims.called


@patch('sweep_builder.scorable._fetch_job_report')
@patch('sweep_builder.scorable.prefer_job_breakdown')
@patch('sweep_builder.scorable.generate_child_claims')
def test_generate_scorable_job_report_prefer_job_breakdown_true(
    mock_generate_child_claims, mock_prefer_job_breakdown, mock_fetch_job_report
):
    mock_prefer_job_breakdown.return_value = False
    mock_fetch_job_report.return_value = sentinel.job_report
    claim = Mock(
        entity_id='entity_id',
        entity_type=Entity.Ad,
        report_type=ReportType.lifetime,
        job_signature=JobSignature('job_id'),
        is_divisible=True,
    )

    result = list(generate_scorable(claim))

    assert len(result) == 1
    assert not mock_generate_child_claims.called


@patch('sweep_builder.scorable._fetch_job_report')
@patch('sweep_builder.scorable.prefer_job_breakdown')
@patch('sweep_builder.scorable.generate_child_claims')
def test_generate_scorable_job_report_prefer_job_breakdown_false(
    mock_generate_child_claims, mock_prefer_job_breakdown, mock_fetch_job_report
):
    mock_prefer_job_breakdown.side_effect = [True, False]
    mock_fetch_job_report.return_value = sentinel.job_report
    mock_generate_child_claims.return_value = [
        Mock(
            entity_id='ad_id',
            entity_type=Entity.Ad,
            report_type=ReportType.lifetime,
            job_signature=JobSignature('job_id'),
        )
    ]

    claim = Mock(
        entity_id='adset_id',
        entity_type=Entity.AdSet,
        report_type=ReportType.lifetime,
        job_signature=JobSignature('job_id'),
        is_divisible=True,
    )

    result = list(generate_scorable(claim))

    assert len(result) == 1
    assert result[0].entity_id == 'ad_id'


@pytest.mark.parametrize(
    'bucket', [FailureBucket.WorkingOnIt, FailureBucket.Success, FailureBucket.Other, FailureBucket.Throttling]
)
def test_prefer_job_breakdown_last_error_not_too_large(bucket):
    report = JobReport(last_failure_bucket=bucket)

    assert not prefer_job_breakdown(report)


def test_prefer_job_breakdown_last_error_is_too_large_none_fails_in_row():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=None)

    assert prefer_job_breakdown(report)


@patch('sweep_builder.scorable.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_prefer_job_breakdown_last_error_is_too_large_none_fails_under_limit():
    report = Mock(last_failure_bucket=FailureBucket.TooLarge, fails_in_row=1)

    assert prefer_job_breakdown(report)


@patch('sweep_builder.scorable.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_prefer_job_breakdown_last_error_is_unrelated_but_over_limit():
    report = Mock(last_failure_bucket=FailureBucket.Throttling, fails_in_row=3)

    assert prefer_job_breakdown(report)

@patch('sweep_builder.scorable.FAILS_IN_ROW_BREAKDOWN_LIMIT', new=2)
def test_prefer_job_breakdown_last_error_is_unrelated_but_under_limit():
    report = Mock(last_failure_bucket=FailureBucket.Throttling, fails_in_row=1)

    assert not prefer_job_breakdown(report)
