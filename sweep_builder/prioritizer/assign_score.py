import logging
import statistics

from datetime import timedelta
from typing import Dict, Tuple

from common.enums.jobtype import JobType, detect_job_type
from common.enums.reporttype import ReportType
from common.measurement import Measure
from common.tztools import now
from config.jobs import ACTIVATE_JOB_GATEKEEPER
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.errors import ScoringException
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper


logger = logging.getLogger(__name__)

SCORE_RANGES: Dict[Tuple[str, str], Tuple[int, int]] = {
    # paid entity and lifetime reports are most important
    (JobType.PAID_DATA, ReportType.entity): (500, 1000),
    (JobType.PAID_DATA, ReportType.lifetime): (400, 900),
    # organic reports less important than paid reports
    (JobType.ORGANIC_DATA, ReportType.entity): (250, 750),
    (JobType.ORGANIC_DATA, ReportType.lifetime): (150, 650),
    # breakdown reports less important than entities or lifetime reports
    (JobType.PAID_DATA, ReportType.day): (100, 600),
    (JobType.PAID_DATA, ReportType.day_age_gender): (100, 600),
    (JobType.PAID_DATA, ReportType.day_dma): (100, 600),
    (JobType.PAID_DATA, ReportType.day_hour): (100, 600),
    (JobType.PAID_DATA, ReportType.day_platform): (100, 600),
}


def _extract_tags_from_claim(claim: ScorableClaim, *_, **__) -> Dict[str, str]:
    return {'entity_type': claim.entity_type, 'ad_account_id': claim.ad_account_id}


def get_score_range(claim: ScorableClaim) -> Tuple[int, int]:
    """Returns score range based on job and report type."""
    job_type = detect_job_type(claim.report_type, claim.report_variant)
    try:
        return SCORE_RANGES[(job_type, claim.report_type)]
    except KeyError:
        raise ScoringException(f'Error scoring job {claim.selected_job_id}')


def historical_ratio(claim: ScorableClaim) -> float:
    """Multiplier based on past efforts to download job."""
    if claim.is_first_attempt:
        return 1.0

    max_dt = now()
    # Absolute minimum is success every 30 days
    min_dt = max_dt - timedelta(days=30)
    last_success_dt = claim.last_report.last_success_dt
    if last_success_dt <= min_dt:
        return 1.0

    # Least recently successful is most important
    min_timestamp = min_dt.timestamp()
    max_timestamp = max_dt.timestamp()
    last_success_timestamp = last_success_dt.timestamp()
    return (max_timestamp - last_success_timestamp) / (max_timestamp - min_timestamp)


def recency_ratio(claim: ScorableClaim) -> float:
    """Multiplier based on how likely to change the data in the report is."""
    # lifetime and entity reports get same score as most recent metric reports
    if claim.range_start is None:
        return 1.0

    max_dt = now().date()
    # Reports older than 2 years ago get minimum
    min_dt = max_dt - timedelta(days=365 * 2)
    if claim.range_start <= min_dt:
        return 0.0

    # Most recent report day is most important
    min_days = min_dt.toordinal()
    max_days = max_dt.toordinal()
    report_days = claim.range_start.toordinal()
    return (report_days - min_days) / (max_days - min_days)


def normalize(value_range: Tuple[int, int], ratio: float) -> int:
    """Returns value in the range linearly scaled with ratio between 0 and 1."""
    score_min, score_max = value_range
    return round((ratio * (score_max - score_min)) + score_min)


@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_from_claim)
def assign_score(claim: ScorableClaim) -> int:
    """Calculate score for a given claim."""
    if claim.report_type in ReportType.MUST_RUN_EVERY_SWEEP:
        return 1000

    if ACTIVATE_JOB_GATEKEEPER and not JobGateKeeper.shall_pass(claim):
        return JobGateKeeper.JOB_NOT_PASSED_SCORE

    score_range = get_score_range(claim)
    hist_ratio = historical_ratio(claim)
    rec_ratio = recency_ratio(claim)

    # equal weight to each ratio
    combined_ratio = statistics.mean([hist_ratio, rec_ratio])

    return normalize(score_range, combined_ratio)
