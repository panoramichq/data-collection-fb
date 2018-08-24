from typing import Generator, Union, List

from common.measurement import Measure
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
    _measurement_name_base = __name__ + '.iter_expectations.'  # <- function name. adjust if changed

    for reality_claim in reality_claims_iter:
        jobs_generators = entity_expectation_generator_map.get(reality_claim.entity_type, [])

        _measurement_tags = dict(
            ad_account_id=reality_claim.ad_account_id,
            entity_type=reality_claim.entity_type
        )

        # histogram measures min/max/ave per thing.
        # Here we are trying to measure how given entity type (per ad account)
        # fans out into expectations.
        with Measure.histogram(_measurement_name_base + 'expectations_per_reality_claim', tags=_measurement_tags) as cntr:
            cnt = 0
            for jobs_generator in jobs_generators:
                yield from jobs_generator(reality_claim)
                cnt += 1

            cntr += cnt
