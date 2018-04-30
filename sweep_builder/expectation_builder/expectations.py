from typing import Generator, Union, List

from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim

from .expectations_inventory import entity_expectation_generator_map


def iter_expectations(reality_claims_iter) :
    # type: (Union[Generator[RealityClaim],List[RealityClaim]]) -> Generator[ExpectationClaim]
    """
    Converts an instance of RealityClaim object (claiming that certain
    entities exist and providing some metadata about their existence)
    into one or more ExpectationClaim objects that express our expectations
    about what report types (for what dates) we expect to see.

    :param reality_claims_iter: Generator yielding RealityClaim objects
    :type reality_claims_iter: Union[Generator[RealityClaim],List[RealityClaim]]
    :return: Generator yielding ExpectationClaim objects
    :rtype: Generator[ExpectationClaim]
    """
    for reality_claim in reality_claims_iter:
        jobs_generators = entity_expectation_generator_map.get(reality_claim.entity_type, [])
        for jobs_generator in jobs_generators:
            yield from jobs_generator(reality_claim)
