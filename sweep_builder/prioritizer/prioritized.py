import logging
import math
import time
from datetime import timedelta

from typing import Generator, Iterable, Tuple, Dict

from common.enums.jobtype import JobType, detect_job_type
from common.enums.reporttype import ReportType
from common.error_inspector import ErrorInspector
from common.measurement import Measure
from common.tztools import now
from config.jobs import ACTIVATE_JOB_GATEKEEPER
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.errors import ScoringException
from sweep_builder.prioritizer.gatekeeper import JobGateKeeper, JobGateKeeperCache

logger = logging.getLogger(__name__)

MAX_SCORE_MULTIPLIER = 1.0
MIN_SCORE_MULTIPLIER = 0.01
JOB_MIN_SUCCESS_PERIOD_IN_DAYS = 30
JOB_MAX_AGE_IN_DAYS = 365 * 2
MUST_RUN_SCORE = 1000


SCORE_RANGES: Dict[Tuple[str, str], Tuple[int, int]] = {
    (JobType.PAID_DATA, ReportType.entity): (500, 1000),
    (JobType.PAID_DATA, ReportType.lifetime): (400, 900),
    (JobType.ORGANIC_DATA, ReportType.entity): (250, 750),
    (JobType.ORGANIC_DATA, ReportType.lifetime): (150, 650),
    (JobType.PAID_DATA, ReportType.day): (100, 600),
    (JobType.PAID_DATA, ReportType.day_age_gender): (100, 600),
    (JobType.PAID_DATA, ReportType.day_dma): (100, 600),
    (JobType.PAID_DATA, ReportType.day_region): (100, 600),
    (JobType.PAID_DATA, ReportType.day_country): (100, 600),
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
        raise ScoringException(f'Error scoring job {claim.job_id}')


def historical_ratio(claim: ScorableClaim) -> float:
    """Multiplier based on past efforts to download job."""
    last_success_dt = claim.last_report and claim.last_report.last_success_dt

    if not last_success_dt:
        return MAX_SCORE_MULTIPLIER
    else:
        # cool! prior success!
        # **the further in the past is prior success the higher is
        # the need to recollect / refresh**
        # (but caps out on constant some ~30 days in past)
        # (not same thing as "the futher in past is the reporting date of data". Separate score there)

        # most recently-successfully collected data score gets close to zero score
        max_dt = now()
        # Absolute minimum is success every N days
        min_dt = max_dt - timedelta(days=JOB_MIN_SUCCESS_PERIOD_IN_DAYS)
        if last_success_dt <= min_dt:
            return MAX_SCORE_MULTIPLIER
        else: #  between min and max
            min_timestamp = min_dt.timestamp()
            max_timestamp = max_dt.timestamp()
            last_success_timestamp = last_success_dt.timestamp()
            return min(
                MAX_SCORE_MULTIPLIER * (max_timestamp - last_success_timestamp) / (max_timestamp - min_timestamp),
                MIN_SCORE_MULTIPLIER
            )


def recency_ratio(claim: ScorableClaim) -> float:
    """Multiplier based on how likely to change the data in the report is."""

    # data points without natural "age" (lifetime metrics and entity reports)
    # must always stay fresh and get full score
    if claim.range_start is None:
        return MAX_SCORE_MULTIPLIER

    # reporting-date-tied records are scored on a downward-sloping line
    # where "half-life" (50% decay) is at JOB_MAX_AGE_IN_DAYS in past

    decay_per_day = 1 / (JOB_MAX_AGE_IN_DAYS * 2)
    today = now().date().toordinal()
    claim_day = claim.range_start.toordinal()
    days_in_past = today - claim_day
    mult = MAX_SCORE_MULTIPLIER * (1 - days_in_past * decay_per_day)

    if mult < MIN_SCORE_MULTIPLIER:
        return MIN_SCORE_MULTIPLIER
    else:
        return mult


def normalize(value_range: Tuple[int, int], ratio: float) -> int:
    """Returns value in the range linearly scaled with ratio between 0 and 1."""
    score_min, score_max = value_range
    return round((ratio * (score_max - score_min)) + score_min)


@Measure.timer(
    __name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_from_claim, sample_rate=0.01
)
def assign_score(claim: ScorableClaim) -> int:
    """Calculate score for a given claim."""
    if claim.report_type in ReportType.MUST_RUN_EVERY_SWEEP:
        return MUST_RUN_SCORE

    if ACTIVATE_JOB_GATEKEEPER and not JobGateKeeperCache.shall_pass(claim.job_id):
        return JobGateKeeperCache.JOB_NOT_PASSED_SCORE

    if ACTIVATE_JOB_GATEKEEPER and not JobGateKeeper.shall_pass(claim):
        return JobGateKeeper.JOB_NOT_PASSED_SCORE

    # score_range = get_score_range(claim)
    hist_ratio = historical_ratio(claim)
    rec_ratio = recency_ratio(claim)

    # equal weight to each ratio
    combined_ratio = hist_ratio * rec_ratio

    return int(MUST_RUN_SCORE * combined_ratio)


def iter_prioritized(claims: Iterable[ScorableClaim]) -> Generator[PrioritizationClaim, None, None]:
    """Assign score for each claim."""
    _measurement_name_base = f'{__name__}.{iter_prioritized.__name__}'

    _before_next_expectation = time.time()

    for claim in claims:
        _measurement_tags = {'entity_type': claim.entity_type, 'ad_account_id': claim.ad_account_id}

        Measure.timing(f'{_measurement_name_base}.next_expected', tags=_measurement_tags, sample_rate=0.01)(
            (time.time() - _before_next_expectation) * 1000
        )

        try:
            score = assign_score(claim)
            with Measure.timer(f'{_measurement_name_base}.yield_result', tags=_measurement_tags):
                yield PrioritizationClaim(
                    claim.entity_id,
                    claim.entity_type,
                    claim.report_type,
                    claim.job_signature,
                    score,
                    ad_account_id=claim.ad_account_id,
                    timezone=claim.timezone,
                    range_start=claim.range_start,
                )
        except ScoringException as e:
            ErrorInspector.inspect(e, claim.ad_account_id, {'job_id': claim.job_id})

        _before_next_expectation = time.time()
