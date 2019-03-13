import functools

from typing import Generator

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.enums.reporttype import ReportType
from common.enums.entity import Entity

from sweep_builder.types import ExpectationGeneratorType


def lifetime_metrics_per_entity(
    entity_type: str, reality_claim: RealityClaim
) -> Generator[ExpectationClaim, None, None]:
    if not reality_claim.timezone:
        # For metrics, reality claim must have timezone.
        return
    assert entity_type in Entity.ALL

    normative_job_id = generate_id(
        ad_account_id=reality_claim.ad_account_id,
        entity_type=reality_claim.entity_type,
        entity_id=reality_claim.entity_id,
        report_type=ReportType.lifetime,
        report_variant=entity_type,
    )

    if reality_claim.ad_account_id == '23845179':
        # Use normative job for ad account 23845179
        yield ExpectationClaim(reality_claim.to_dict(), normative_job_signature=JobSignature(normative_job_id))
    else:
        yield ExpectationClaim(
            reality_claim.to_dict(),
            normative_job_signature=JobSignature(normative_job_id),
            effective_job_signatures=[
                JobSignature(
                    generate_id(
                        ad_account_id=reality_claim.ad_account_id,
                        report_type=ReportType.lifetime,
                        report_variant=entity_type,
                    )
                )
            ],
        )


lifetime_metrics_per_campaign: ExpectationGeneratorType = functools.partial(
    lifetime_metrics_per_entity, Entity.Campaign
)

lifetime_metrics_per_adset: ExpectationGeneratorType = functools.partial(lifetime_metrics_per_entity, Entity.AdSet)

lifetime_metrics_per_ad: ExpectationGeneratorType = functools.partial(lifetime_metrics_per_entity, Entity.Ad)
