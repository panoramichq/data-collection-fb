from typing import Generator, Iterable

from common.measurement import Measure
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectations_inventory.inventory import (
    # entity_expectations_for_23845179,
    entity_expectation_generator_map,
)


def iter_expectations(reality_claims_iter: Iterable[RealityClaim]) -> Generator[ExpectationClaim, None, None]:
    """
    Converts an instance of RealityClaim object (claiming that certain
    entities exist and providing some metadata about their existence)
    into one or more ExpectationClaim objects that express our expectations
    about what report types (for what dates) we expect to see.
    """
    for reality_claim in reality_claims_iter:
        # if reality_claim.ad_account_id == '23845179':
        #     jobs_generators = entity_expectations_for_23845179.get(reality_claim.entity_type, [])
        # else:
        jobs_generators = entity_expectation_generator_map.get(reality_claim.entity_type, [])

        count = 0

        for jobs_generator in jobs_generators:
            for expectation_claim in jobs_generator(reality_claim):
                yield expectation_claim
                count += 1

        Measure.histogram(
            f'{__name__}.{iter_expectations.__name__}.expectations_per_reality_claim',
            tags={'ad_account_id': reality_claim.ad_account_id, 'entity_type': reality_claim.entity_type},
        )(count)
