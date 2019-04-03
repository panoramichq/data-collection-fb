import logging

from typing import Generator, Union, Iterable

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.prioritization_claim import PrioritizationClaim

from sweep_builder.persister import iter_persist_prioritized
from sweep_builder.prioritizer.prioritized import iter_prioritized
from sweep_builder.expectation_builder.expectations import iter_expectations

logger = logging.getLogger(__name__)


def iter_pipeline(
    sweep_id: str, reality_claims_iter: Union[Generator[RealityClaim, None, None], Iterable[RealityClaim]]
) -> Generator[PrioritizationClaim, None, None]:
    """
    Convenience method. Packs together multiple layers
    of code that unpack RealityClaim into expectations,
    prioritization claims and associated processing steps.
    """
    yield from iter_persist_prioritized(sweep_id, iter_prioritized(iter_expectations(reality_claims_iter)))
