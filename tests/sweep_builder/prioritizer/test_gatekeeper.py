import pytest

from unittest.mock import Mock
from datetime import timedelta

from common.enums.reporttype import ReportType
from common.tztools import now
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper


def test_shall_pass_last_success_dt_and_last_progress_dt_none_returns_true():
    assert JobGateKeeper.shall_pass(Mock(last_report=Mock(last_progress_dt=None, last_success_dt=None)))


@pytest.mark.parametrize(
    ['range_start_delta', 'last_success_delta', 'expected'],
    [
        (timedelta(days=6), timedelta(hours=4), True),
        (timedelta(days=6), timedelta(hours=2), False),
        (timedelta(days=13), timedelta(hours=11), True),
        (timedelta(days=13), timedelta(hours=9), False),
        (timedelta(days=29), timedelta(hours=26), True),
        (timedelta(days=29), timedelta(hours=23), False),
        (timedelta(days=89), timedelta(hours=24 * 7 + 1), True),
        (timedelta(days=89), timedelta(hours=24 * 7 - 1), False),
        (timedelta(days=91), timedelta(hours=24 * 7 * 3 + 1), True),
        (timedelta(days=91), timedelta(hours=24 * 7 * 3 - 1), False),
    ],
)
def test_shall_pass_range_end_less_than_seven_days_true(range_start_delta, last_success_delta, expected):
    """Check range_start now - delta and last_success now - delta returns expected."""
    last_report = Mock(last_success_dt=now() - last_success_delta, last_progress_dt=None)
    claim = Mock(last_report=last_report, range_start=(now() - range_start_delta).date(), range_end=None)

    assert expected == JobGateKeeper.shall_pass(claim)


@pytest.mark.parametrize(['last_success_delta', 'expected'], [(timedelta(hours=7), True), (timedelta(hours=5), False)])
def test_shall_pass_lifetime_report_type(last_success_delta, expected):
    """Check behaviour for lifetime report type"""
    last_report = Mock(last_success_dt=now() - last_success_delta, last_progress_dt=None)
    claim = Mock(last_report=last_report, range_start=None, range_end=None, report_type=ReportType.lifetime)

    assert expected == JobGateKeeper.shall_pass(claim)


@pytest.mark.parametrize(['last_success_delta', 'expected'], [(timedelta(hours=3), True), (timedelta(hours=1), False)])
def test_shall_pass_entity_report_type(last_success_delta, expected):
    """Check behaviour for entity report type"""
    last_report = Mock(last_success_dt=now() - last_success_delta, last_progress_dt=None)
    parts = Mock(range_start=None, range_end=None, report_type=ReportType.entity, last_report=last_report)

    assert expected == JobGateKeeper.shall_pass(parts)


def test_shall_pass_progress_reported_recently():
    last_report = Mock(last_success_dt=None, last_progress_dt=now() - timedelta(minutes=3))
    claim = Mock(range_start=None, range_end=None, report_type=ReportType.entity, last_report=last_report)

    assert JobGateKeeper.shall_pass(claim) is False


def test_shall_pass_progress_reported_long_ago():
    last_report = Mock(last_success_dt=None, last_progress_dt=now() - timedelta(minutes=10))
    parts = Mock(range_start=None, range_end=None, report_type=ReportType.entity, last_report=last_report)

    assert JobGateKeeper.shall_pass(parts) is True
