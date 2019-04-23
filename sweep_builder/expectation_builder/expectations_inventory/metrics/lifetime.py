import functools

from typing import Generator

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.enums.reporttype import ReportType
from common.enums.entity import Entity

from sweep_builder.types import ExpectationGeneratorType


def lifetime_metrics_per_entity_under_ad_account(
    entity_type: str, reality_claim: RealityClaim
) -> Generator[ExpectationClaim, None, None]:
    """Generate ad-account level expectation claims for lifetime."""
    if not reality_claim.timezone:
        return

    if reality_claim.ad_account_id == '23845179':
        # Use normative job for ad account 23845179
        yield ExpectationClaim(
            reality_claim.entity_id,
            reality_claim.entity_type,
            ReportType.lifetime,
            JobSignature(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    entity_type=reality_claim.entity_type,
                    entity_id=reality_claim.entity_id,
                    report_type=ReportType.lifetime,
                    report_variant=entity_type,
                )
            ),
            ad_account_id=reality_claim.ad_account_id,
            timezone=reality_claim.timezone,
            report_variant=entity_type,
        )
    else:
        yield ExpectationClaim(
            reality_claim.entity_id,
            reality_claim.entity_type,
            ReportType.lifetime,
            JobSignature(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    report_type=ReportType.lifetime,
                    report_variant=entity_type,
                )
            ),
            ad_account_id=reality_claim.ad_account_id,
            timezone=reality_claim.timezone,
            report_variant=entity_type,
        )


def lifetime_page_metrics_per_entity(
    entity_type: str, reality_claim: RealityClaim
) -> Generator[ExpectationClaim, None, None]:
    assert entity_type in Entity.ALL

    yield ExpectationClaim(
        reality_claim.entity_id,
        reality_claim.entity_type,
        ReportType.lifetime,
        JobSignature(
            generate_id(
                ad_account_id=reality_claim.ad_account_id,
                entity_type=reality_claim.entity_type,
                entity_id=reality_claim.entity_id,
                report_type=ReportType.lifetime,
                report_variant=entity_type,
            )
        ),
        ad_account_id=reality_claim.ad_account_id,
    )


lifetime_metrics_per_ads_under_ad_account = functools.partial(lifetime_metrics_per_entity_under_ad_account, Entity.Ad)
lifetime_metrics_per_adsets_under_ad_account = functools.partial(
    lifetime_metrics_per_entity_under_ad_account, Entity.AdSet
)
lifetime_metrics_per_campaigns_under_ad_account = functools.partial(
    lifetime_metrics_per_entity_under_ad_account, Entity.Campaign
)

lifetime_metrics_per_page_video: ExpectationGeneratorType = functools.partial(
    lifetime_page_metrics_per_entity, Entity.PageVideo
)

lifetime_metrics_per_page: ExpectationGeneratorType = functools.partial(lifetime_page_metrics_per_entity, Entity.Page)
lifetime_metrics_per_page_post: ExpectationGeneratorType = functools.partial(
    lifetime_page_metrics_per_entity, Entity.PagePost
)
