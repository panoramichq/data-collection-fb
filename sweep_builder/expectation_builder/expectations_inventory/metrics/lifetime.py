import functools
import logging

from typing import Generator

from sweep_builder.data_containers.entity_node import EntityNode
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.enums.reporttype import ReportType
from common.enums.entity import Entity
from sweep_builder.reality_inferrer.reality import iter_reality_per_ad_account_claim

from sweep_builder.types import ExpectationGeneratorType

logger = logging.getLogger(__name__)


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

        # TODO: Remove once all entities have parent ids
        # Divide tasks only if parent levels are defined for all ads
        is_dividing_possible = True

        root_node = EntityNode(reality_claim.entity_id, reality_claim.entity_type)
        for child_claim in iter_reality_per_ad_account_claim(reality_claim, entity_types=[entity_type]):
            is_dividing_possible = is_dividing_possible and child_claim.is_divisible
            if is_dividing_possible:
                new_node = EntityNode(child_claim.entity_id, child_claim.entity_type)
                root_node.add_node(new_node, path=child_claim.parent_entity_ids)

        logger.warning(
            f'[dividing-possible] Ad Account {reality_claim.ad_account_id} Dividing possible: {is_dividing_possible}'
        )

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
            entity_hierarchy=root_node if is_dividing_possible else None,
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
