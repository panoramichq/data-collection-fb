from unittest.mock import patch, Mock

import pytest

from datetime import timedelta

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.job_signature import JobSignature
from common.store.jobreport import JobReport
from common.tztools import now
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.errors import ScoringException
from sweep_builder.prioritizer.prioritized import (
    iter_prioritized,
    assign_score,
    normalize,
    historical_ratio,
    recency_ratio,
    JOB_MIN_SUCCESS_PERIOD_IN_DAYS,
    JOB_MAX_AGE_IN_DAYS,
)
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper


# fmt: off
@pytest.mark.parametrize(
    ['value_range', 'ratio', 'expected'],
    [
        ((0, 100), 0.5, 50.0),
        ((100, 600), 0.5, 350.0),
        ((100, 600), 0, 100),
        ((100, 600), 1, 600),
    ],
)
def test_normalize(value_range, ratio, expected):
    assert normalize(value_range, ratio) == expected


@pytest.mark.parametrize(
    ['range_start', 'expected'],
    [
        (None, 1.0),
        (now().date() - timedelta(days=JOB_MAX_AGE_IN_DAYS), 0.5),
        (now().date() - timedelta(days=JOB_MAX_AGE_IN_DAYS * 2 +1), 0.01),
        (now().date(), 1.0),
    ]
)
def test_recency_ratio(range_start, expected):
    signature = JobSignature('jobid')
    claim = ScorableClaim('A1', Entity.Ad, ReportType.lifetime, Entity.Ad, signature, None, range_start=range_start)

    score = recency_ratio(claim)

    assert score == pytest.approx(expected, abs=0.01)


@pytest.mark.parametrize(
    ['last_success_dt', 'expected'],
    [
        (None, 1.0),
        (now() - timedelta(days=JOB_MIN_SUCCESS_PERIOD_IN_DAYS + 1), 1.0),
        (now(), 0.0),
    ]
)
def test_historical_ratio(last_success_dt, expected):
    signature = JobSignature('jobid')
    last_report = JobReport(last_success_dt=last_success_dt)
    claim = ScorableClaim('A1', Entity.Ad, ReportType.lifetime, Entity.Ad, signature, last_report)

    score = historical_ratio(claim)

    assert score == pytest.approx(expected, abs=0.01)

# fmt: on


@patch.object(JobGateKeeper, 'shall_pass', return_value=True)
@patch('sweep_builder.prioritizer.prioritized.historical_ratio', return_value=0.75)
@patch('sweep_builder.prioritizer.prioritized.recency_ratio', return_value=0.25)
def test_assign_score(*_):
    claim = ScorableClaim('A1', Entity.Ad, ReportType.lifetime, Entity.Ad, JobSignature('jobid'), None)

    result = assign_score(claim)

    assert result == 187


@patch('sweep_builder.prioritizer.prioritized.assign_score')
def test_iter_prioritized_assign_score_throws_keeps_going(mock_assign_score):
    mock_assign_score.side_effect = [ScoringException('test'), 10]

    results = [pc.score for pc in iter_prioritized([Mock(), Mock()])]

    assert results == [10]
