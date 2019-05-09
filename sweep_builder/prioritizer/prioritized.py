import time

from typing import Generator, Iterable

from common.measurement import Measure
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.prioritizer.assign_score import assign_score


def iter_prioritized(claims: Iterable[ScorableClaim]) -> Generator[PrioritizationClaim, None, None]:
    """Assign score for each claim."""
    _measurement_name_base = f'{__name__}.{iter_prioritized.__name__}'

    _before_next_expectation = time.time()

    for claim in claims:
        _measurement_tags = {'entity_type': claim.entity_type, 'ad_account_id': claim.ad_account_id}

        Measure.timing(f'{_measurement_name_base}.next_expected', tags=_measurement_tags)(
            (time.time() - _before_next_expectation) * 1000
        )

        job_signature = claim.job_signature

        score = assign_score(claim)

        with Measure.timer(f'{_measurement_name_base}.yield_result', tags=_measurement_tags):
            yield PrioritizationClaim(
                claim.entity_id,
                claim.entity_type,
                claim.report_type,
                job_signature,
                score,
                ad_account_id=claim.ad_account_id,
                timezone=claim.timezone,
            )

        _before_next_expectation = time.time()
