from collections import defaultdict
from typing import Generator, Iterable

from common.enums.jobtype import detect_job_type
from common.measurement import Measure
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectations_inventory.inventory import entity_expectation_generator_map


def iter_expectations(reality_claims_iter: Iterable[RealityClaim]) -> Generator[ExpectationClaim, None, None]:
    """
    Converts an instance of RealityClaim object (claiming that certain
    entities exist and providing some metadata about their existence)
    into one or more ExpectationClaim objects that express our expectations
    about what report types (for what dates) we expect to see.
    """
    histogram_counter = defaultdict(int)
    for claim in reality_claims_iter:
        jobs_generators = entity_expectation_generator_map.get(claim.entity_type, [])
        for jobs_generator in jobs_generators:
            for expectation_claim in jobs_generator(claim):
                yield expectation_claim
                job_type = detect_job_type(expectation_claim.report_type, expectation_claim.entity_type)
                histogram_counter[(claim.ad_account_id, claim.entity_type, job_type)] += 1

    for ((ad_account_id, entity_type, job_type), count) in histogram_counter.items():
        Measure.histogram(
            f'{__name__}.{iter_expectations.__name__}.expectations_per_reality_claim',
            tags={'ad_account_id': ad_account_id, 'entity_type': entity_type, 'job_type': job_type},
        )(count)
