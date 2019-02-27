import logging

from typing import Iterator, Generator

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from .data_containers.prioritization_claim import PrioritizationClaim

from .persister import iter_persist_prioritized
from .prioritizer.prioritized import iter_prioritized
from .expectation_builder.expectations import iter_expectations

logger = logging.getLogger(__name__)


def iter_dedup_expectations(expectation_claims):
    # type: (Iterator[ExpectationClaim]) -> Generator[ExpectationClaim]
    """Dedup expectation claims based on effective job signature id."""
    total_count = 0
    unique_count = 0
    seen_job_ids = set()
    for claim in expectation_claims:
        total_count += 1
        effective_signature = claim.job_signatures[-1].job_id
        if effective_signature not in seen_job_ids:
            unique_count += 1
            seen_job_ids.add(effective_signature)
            yield claim

        if total_count % 100 == 0:
            logger.info(f'total: {total_count}, unique: {unique_count}')


def iter_pipeline(sweep_id, reality_claims_iter):
    # type: (str, Generator[RealityClaim]) -> Generator[PrioritizationClaim]
    """
    Convenience method. Packs together multiple layers
    of code that unpack RealityClaim into expectations,
    prioritization claims and associated processing steps.

    :param str sweep_id:
    :param reality_claims_iter: Iterable yielding RealityClaim objects
    :type reality_claims_iter: Generator[RealityClaim]
    :rtype: Generator[PrioritizationClaim]
    """
    yield from iter_persist_prioritized(
        sweep_id,
        iter_prioritized(
            iter_dedup_expectations(
                iter_expectations(
                    reality_claims_iter
                )
            )
        )
    )
