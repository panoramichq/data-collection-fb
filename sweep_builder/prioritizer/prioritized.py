import time

from typing import Generator, Iterable

from common.measurement import Measure
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim
from sweep_builder.data_containers.scorable_claim import ScorableClaim
from sweep_builder.prioritizer.assign_score import assign_score


def iter_prioritized(claims: Iterable[ScorableClaim]) -> Generator[PrioritizationClaim, None, None]:
    """Assign score for each claim."""
    _measurement_name_base = f"{__name__}.{iter_prioritized.__name__}"
    _measurement_sample_rate = 1

    _before_next_expectation = time.time()

    seen_job_ids = set()

    for claim in claims:
        _measurement_tags = {"entity_type": claim.entity_type, "ad_account_id": claim.ad_account_id}

        Measure.timing(
            f"{_measurement_name_base}.next_expected", tags=_measurement_tags, sample_rate=_measurement_sample_rate
        )((time.time() - _before_next_expectation) * 1000)

        # Skip already seen job_ids
        if claim.selected_signature.job_id in seen_job_ids:
            continue

        with Measure.timer(
            f"{_measurement_name_base}.assign_score", tags=_measurement_tags, sample_rate=_measurement_sample_rate
        ):
            score = assign_score(claim)

        with Measure.timer(
            f"{_measurement_name_base}.yield_result", tags=_measurement_tags, sample_rate=_measurement_sample_rate
        ):
            yield PrioritizationClaim(claim.to_dict(), score=score)

        _before_next_expectation = time.time()
