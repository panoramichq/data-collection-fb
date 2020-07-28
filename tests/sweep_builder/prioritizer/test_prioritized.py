import pytest

from datetime import timedelta, datetime
from unittest.mock import patch, Mock

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.job_signature import JobSignature
from common.store.jobreport import JobReport
from common.tztools import now
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.errors import ScoringException
from sweep_builder.prioritizer.prioritized import (
    JOB_MAX_AGE_IN_DAYS,
    JOB_MIN_SUCCESS_PERIOD_IN_DAYS,
    MUST_RUN_SCORE,
    ScoreCalculator,
    ScoreSkewHandlers,
)

@pytest.mark.parametrize(
    ['last_success_dt', 'expected'],
    [
        (None, 1.0),
        (now() - timedelta(days=JOB_MIN_SUCCESS_PERIOD_IN_DAYS + 1), 0.5),
        (now(), 0.0),
    ]
)
def test_historical_ratio(last_success_dt, expected):
    signature = JobSignature('jobid')
    last_report = JobReport(last_success_dt=last_success_dt)
    claim = ScorableClaim('A1', Entity.Ad, ReportType.lifetime, Entity.Ad, signature, last_report)

    score = ScoreCalculator.historical_ratio(claim)

    assert score == pytest.approx(expected, abs=0.01)


@pytest.mark.parametrize(
    ['report_type', 'historical_ratio', 'skew_ratio', 'score'],
    [
        (ReportType.lifetime, 1.0, 1.0, MUST_RUN_SCORE),
        (ReportType.lifetime, 1.0, 0.5, MUST_RUN_SCORE * 0.5),
        (ReportType.lifetime, 0.6, 0.2, MUST_RUN_SCORE * 0.12),
        (ReportType.entity, 1.0, 1.0, MUST_RUN_SCORE),
        (ReportType.entity, 1.0, 0.5, MUST_RUN_SCORE * 0.5),
        (ReportType.entity, 0.6, 0.2, MUST_RUN_SCORE * 0.12),
    ]
)
def test_assign_score(report_type, historical_ratio, skew_ratio, score):
    claim = ScorableClaim('A1', Entity.Ad, report_type, Entity.Ad, JobSignature('jobid'), None)
    with patch.object(ScoreCalculator, 'historical_ratio', return_value=historical_ratio), \
            patch.object(ScoreCalculator, 'skew_ratio', return_value=skew_ratio):

        result = ScoreCalculator.assign_score(claim)
    assert result == pytest.approx(score, abs=0.1)


@pytest.mark.parametrize(
    ['dt', 'expected_score'],
    [
        (datetime(2000, 1, 1, 1, 0), 1.0),
        (datetime(2000, 1, 1, 1, 5), 0.83),
        (datetime(2000, 1, 1, 1, 10), 0.67),
        (datetime(2000, 1, 1, 1, 15), 0.5),
        (datetime(2000, 1, 1, 1, 20), 0.33),
        (datetime(2000, 1, 1, 1, 25), 0.17),
        (datetime(2000, 1, 1, 1, 30), 0.0),
        (datetime(2000, 1, 1, 1, 35), 0.17),
        (datetime(2000, 1, 1, 1, 40), 0.33),
        (datetime(2000, 1, 1, 1, 45), 0.5),
        (datetime(2000, 1, 1, 1, 50), 0.67),
        (datetime(2000, 1, 1, 1, 55), 0.83),
    ]
)
def test_lifetime_score(dt, expected_score):
    signature = JobSignature('jobid')
    claim = ScorableClaim('A1', Entity.Ad, ReportType.lifetime, Entity.Ad, signature, None)
    with patch.object(ScoreSkewHandlers, 'get_now', return_value=dt) as mm:
        score = ScoreSkewHandlers.lifetime_score(claim=claim)

    assert mm.called
    assert score == pytest.approx(expected_score, abs=0.01)
