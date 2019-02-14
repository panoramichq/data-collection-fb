from typing import Generator, Union, List
from collections import defaultdict

from common.enums.entity import Entity
from common.id_tools import parse_id_parts
from common.measurement import Measure
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectations_inventory.inventory import ad_account_entity_collection_job_set, \
    campaign_metrics_job_set, adset_metrics_job_set

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
    _measurement_sample_rate = 1

    for reality_claim in reality_claims_iter:
        if reality_claim.ad_account_id == '23845179':
            if reality_claim.entity_type == Entity.AdAccount:
                # For ad account download entities and sync expectations
                jobs_generators = ad_account_entity_collection_job_set
            elif reality_claim.entity_type == Entity.Campaign:
                # For campaigns generate jobs to download reports
                jobs_generators = campaign_metrics_job_set
            elif reality_claim.entity_type == Entity.AdSet:
                # For adsets generate dma jobs
                jobs_generators = adset_metrics_job_set
            else:
                # For other entities we are covered by above
                continue
        else:
            # For other accounts, proceed as usual
            jobs_generators = entity_expectation_generator_map.get(reality_claim.entity_type, [])

        # histogram measures min/max/ave per thing.
        # Here we are trying to measure how given entity type (per ad account)
        # fans out into expectations.
        counts = defaultdict(int)

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
