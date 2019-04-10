import pytest

from unittest.mock import Mock
from datetime import timedelta

from common.enums.reporttype import ReportType
from common.tztools import now
from sweep_builder.prioritizer.assign_score import JobGateKeeper


def test_allow_normal_score_last_success_dt_none_returns_true():
    assert JobGateKeeper.allow_normal_score(Mock(), None)


def test_allow_normal_score_range_end_less_than_three_days_ago_returns_true():
    parts = Mock(range_end=None, range_start=(now() - timedelta(days=2)).date())
    assert JobGateKeeper.allow_normal_score(parts, now())


@pytest.mark.parametrize(
    ['range_start_delta', 'last_success_delta', 'expected'],
    [
        (timedelta(days=6), timedelta(hours=2), True),
        (timedelta(days=6), timedelta(hours=0.5), False),
        (timedelta(days=13), timedelta(hours=6), True),
        (timedelta(days=13), timedelta(hours=4), False),
        (timedelta(days=29), timedelta(hours=26), True),
        (timedelta(days=29), timedelta(hours=23), False),
        (timedelta(days=89), timedelta(hours=24 * 3 + 1), True),
        (timedelta(days=89), timedelta(hours=24 * 3 - 1), False),
        (timedelta(days=91), timedelta(hours=24 * 7 + 1), True),
        (timedelta(days=91), timedelta(hours=24 * 7 - 1), False),
    ],
)
def test_allow_normal_score_range_end_less_than_seven_days_true(range_start_delta, last_success_delta, expected):
    """Check range_start now - delta and last_success now - delta returns expected."""
    parts = Mock(range_end=None, range_start=(now() - range_start_delta).date())

    assert expected == JobGateKeeper.allow_normal_score(parts, now() - last_success_delta)


@pytest.mark.parametrize(['last_success_delta', 'expected'], [(timedelta(hours=7), True), (timedelta(hours=5), False)])
def test_allow_normal_score_lifetime_report_type(last_success_delta, expected):
    """Check behaviour for lifetime report type"""
    parts = Mock(range_start=None, range_end=None, report_type=ReportType.lifetime)

    assert expected == JobGateKeeper.allow_normal_score(parts, now() - last_success_delta)


@pytest.mark.parametrize(['last_success_delta', 'expected'], [(timedelta(hours=3), True), (timedelta(hours=1), False)])
def test_allow_normal_score_entity_report_type(last_success_delta, expected):
    """Check behaviour for entity report type"""
    parts = Mock(range_start=None, range_end=None, report_type=ReportType.entity)

    assert expected == JobGateKeeper.allow_normal_score(parts, now() - last_success_delta)
