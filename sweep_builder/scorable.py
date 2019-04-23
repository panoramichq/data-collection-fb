import functools
import logging

from collections import defaultdict
from typing import Iterable, Generator, Optional

from pynamodb.exceptions import DoesNotExist

from common.enums.jobtype import detect_job_type
from config.application import PERMANENTLY_FAILING_JOB_THRESHOLD
from common.measurement import Measure
from common.store.jobreport import JobReport
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=None)
def _fetch_job_report(job_id: str) -> Optional[JobReport]:
    """Retrieve job report from job report table (cached)."""
    try:
        report = JobReport.get(job_id)
        if report.fails_in_row and report.fails_in_row >= PERMANENTLY_FAILING_JOB_THRESHOLD:
            Measure.counter('permanently_failing_job').increment()
            logger.warning(
                f'[permanently-failing-job] Job with id {job_id} failed {report.fails_in_row}' f' times in a row.'
            )
        return report
    except DoesNotExist:
        return None


def generate_scorable(claim: ExpectationClaim) -> Generator[ScorableClaim, None, None]:
    """Select job signature for single expectation claim."""
    last_report = _fetch_job_report(claim.job_id)
    yield ScorableClaim(
        claim.entity_id,
        claim.entity_type,
        claim.report_type,
        claim.job_signature,
        last_report,
        ad_account_id=claim.ad_account_id,
        timezone=claim.timezone,
    )


def iter_scorable(claims: Iterable[ExpectationClaim]) -> Generator[ScorableClaim, None, None]:
    """Select signature for each expectation claim based on job history."""
    histogram_counter = defaultdict(int)
    for claim in claims:
        for scorable_claim in generate_scorable(claim):
            job_type = detect_job_type(claim.report_type, claim.entity_type)
            histogram_counter[(claim.ad_account_id, claim.entity_type, job_type)] += 1
            yield scorable_claim

    for ((ad_account_id, entity_type, job_type), count) in histogram_counter.items():
        Measure.histogram(
            f'{__name__}.{iter_scorable.__name__}.scorable_claims_per_expectation_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type, 'job_type': job_type},
        )(count)
