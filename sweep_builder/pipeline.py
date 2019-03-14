import logging

from typing import Generator, Union, Iterable

from sweep_builder.data_containers.reality_claim import RealityClaim
from .data_containers.prioritization_claim import PrioritizationClaim

from .persister import iter_persist_prioritized
from .prioritizer.prioritized import iter_prioritized
from .expectation_builder.expectations import iter_expectations

logger = logging.getLogger(__name__)


def iter_pipeline(
    sweep_id: str,
    reality_claims_iter: Union[Generator[RealityClaim, None, None], Iterable[RealityClaim]],
) -> Generator[PrioritizationClaim, None, None]:
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
            iter_expectations(
                reality_claims_iter
            )
        )
    )
