import itertools
from typing import Generator, Union, List
from collections import defaultdict

from common.enums.entity import Entity
from common.id_tools import parse_id_parts
from common.measurement import Measure
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectations_inventory.entities import custom_audience_entities_per_ad_account
from sweep_builder.expectation_builder.expectations_inventory.metrics import breakdowns

from .expectations_inventory import entity_expectation_generator_map


def iter_expectations(reality_claims_iter):
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
    _measurement_sample_rate = 1

    for reality_claim in reality_claims_iter:
        jobs_generators = entity_expectation_generator_map.get(reality_claim.entity_type, [])

        # histogram measures min/max/ave per thing.
        # Here we are trying to measure how given entity type (per ad account)
        # fans out into expectations.
        counts = defaultdict(int)

        # Temporary fix for ad account id 23845179
        if reality_claim.entity_type == Entity.Campaign and reality_claim.ad_account_id == '23845179':
            jobs_generators = itertools.chain(jobs_generators, [
                breakdowns.hour_metrics_per_adset_per_entity,
                breakdowns.day_metrics_per_ad_per_entity,
                breakdowns.hour_metrics_per_ad_per_entity,
                breakdowns.day_age_gender_metrics_per_ad_per_entity,
                breakdowns.day_dma_metrics_per_ad_per_entity,
                breakdowns.day_platform_metrics_per_ad_per_entity,
            ])

        for jobs_generator in jobs_generators:
            for expectation_claim in jobs_generator(reality_claim):  # type: ExpectationClaim
                yield expectation_claim

                if len(expectation_claim.job_signatures):
                    job_signature = expectation_claim.job_signatures[-1]
                    report_type = parse_id_parts(job_signature.job_id).report_type
                else:
                    report_type = 'unknown'

                counts[report_type] += 1

        for report_type, cnt in counts.items():
            Measure.histogram(
                _measurement_name_base + 'expectations_per_reality_claim',
                tags=dict(
                    ad_account_id=reality_claim.ad_account_id,
                    entity_type=reality_claim.entity_type,
                    report_type=report_type
                ),
                sample_rate=_measurement_sample_rate
            )(cnt)
