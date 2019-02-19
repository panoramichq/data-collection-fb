import logging

from typing import Generator

from common.enums.entity import Entity
from oozer.common.expecations_store import JobExpectationsWriter
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from sweep_builder.data_containers.reality_claim import RealityClaim
from .data_containers.prioritization_claim import PrioritizationClaim

from .persister import iter_persist_prioritized
from .prioritizer.prioritized import iter_prioritized
from .expectation_builder.expectations import iter_expectations

logger = logging.getLogger(__name__)


# :) Guess what for
FIRST = 0
LAST = -1


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
            iter_expectations(
                reality_claims_iter
            )
        )
    )
