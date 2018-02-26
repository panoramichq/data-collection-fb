from typing import Generator, Callable

from sweep_builder.reality_inferrer.reality import iter_reality, RealityClaim

from .expectation_claim import ExpectationClaim
from .report_inventory import entity_jobs_gens_map

def iter_expectations(iter_reality=iter_reality):
    # type: (Callable[..., Generator[RealityClaim]]) -> Generator[ExpectationClaim]
    """
    Converts a stream of RealityClaim objects (claiming that certain
    entities exist and providing some metadata about their existence)
    into a stream of ExpectationClaim objects that express our expectations
    about what report types (for what dates) we expect to see.

    :param iter_reality: Callable that returns generator yielding RealityClaim objects
    :type iter_reality: () -> Generator[RealityClaim]
    :return: Generator yielding ExpectationClaim objects
    :rtype: Generator[ExpectationClaim]
    """

    for reality_claim in iter_reality():
        jobs_generators = entity_jobs_gens_map[reality_claim.entity_type]
        for jobs_generator in jobs_generators:
            yield from jobs_generator(reality_claim)
