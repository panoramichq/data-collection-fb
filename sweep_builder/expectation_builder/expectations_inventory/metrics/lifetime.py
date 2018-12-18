import functools

from typing import Generator

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.enums.reporttype import ReportType
from common.enums.entity import Entity


def lifetime_metrics_per_entity(entity_type, reality_claim):
    # type: (str, RealityClaim) -> Generator[ExpectationClaim]
    """
    :param str entity_type: One of Entity enum values
    :param RealityClaim reality_claim:
    :rtype: Generator[ExpectationClaim]
    """
    if not reality_claim.timezone:
        # For metrics, reality claim must have timezone.
        return
    assert entity_type in Entity.ALL

    normative_job_id = generate_id(
        ad_account_id=reality_claim.ad_account_id,
        entity_type=entity_type,
        entity_id=reality_claim.entity_id,
        report_type=ReportType.lifetime
    )

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures = [
            # normative job signature
            JobSignature.bind(
                normative_job_id
            ),
            # possible alternative "effective" job signatures:
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    report_type=ReportType.lifetime,
                    report_variant=entity_type
                ),
                normative_job_id=normative_job_id
            )
        ]
    )


lifetime_metrics_per_campaign = functools.partial(
    lifetime_metrics_per_entity,
    Entity.Campaign
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


lifetime_metrics_per_adset = functools.partial(
    lifetime_metrics_per_entity,
    Entity.AdSet
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


lifetime_metrics_per_ad = functools.partial(
    lifetime_metrics_per_entity,
    Entity.Ad
)  # type: (RealityClaim) -> Generator[ExpectationClaim]
