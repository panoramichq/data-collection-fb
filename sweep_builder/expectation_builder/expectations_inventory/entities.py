import functools

from typing import Generator

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.enums.reporttype import ReportType
from common.enums.entity import Entity
from sweep_builder.types import ExpectationGeneratorType


def entities_per_ad_account(entity_type: str, reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    """
    Generates "fetch EntityType entities metadata per given AA" job call sig
    """
    # Mental Note:
    # This job signature generator is designed to be parked
    # *under AdAccount job signatures generators inventory*
    # In other words, NOT under EntityType inventory (where it would be called
    # for each EntityType).
    # Unlike metrics report types,
    # We don't have an effective "fetch single EntityType entity data per EntityType ID" task (yet).
    # So, instead of generating many job signatures per EntityTypes,
    # we create only one per-parent-AA, and making that
    # into "normative_job_signature" per AA level.
    # When we have a need to have atomic per-EntityType entity data collection celery task,
    # atomic per-C entity data job signature would go into normative column
    # on ExpectationClaim for each and separate EntityType and per-parent-AA
    # job signature will go into "effective_job_signatures" list on those claims,
    # AND this function must move from AA-level to EntityType-level signature
    # generators inventory.

    assert entity_type in Entity.ALL

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type
                )
            )
        ]
    )


def entities_per_page(entity_type: str, reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    """
    Generates "fetch EntityType entities metadata per given Page" job call sig
    """
    assert entity_type in Entity.NON_AA_SCOPED

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type
                )
            )
        ]
    )


def entities_per_page_post(entity_type: str, reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    """
    Generates "fetch EntityType entities metadata per given Page" job call sig
    """
    assert entity_type in Entity.NON_AA_SCOPED

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type,
                    entity_id=reality_claim.entity_id,
                )
            )
        ]
    )


def page_entity(reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    assert reality_claim.entity_type == Entity.Page, \
        'Page expectation should be triggered only by page reality claims'

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    entity_id=reality_claim.entity_id,
                    report_type=ReportType.entity,
                    report_variant=Entity.Page
                )
            )
        ]
    )


def ad_account_entity(reality_claim: RealityClaim) -> Generator[ExpectationClaim, None, None]:
    assert reality_claim.entity_type == Entity.AdAccount, \
        'Ad account expectation should be triggered only by ad account reality claims'

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    entity_id=reality_claim.entity_id,
                    report_type=ReportType.entity,
                    report_variant=Entity.AdAccount
                )
            )
        ]
    )


ad_account: ExpectationGeneratorType = functools.partial(entities_per_ad_account, Entity.AdAccount)

campaign_entities_per_ad_account: ExpectationGeneratorType = functools.partial(entities_per_ad_account, Entity.Campaign)

adset_entities_per_ad_account: ExpectationGeneratorType = functools.partial(entities_per_ad_account, Entity.AdSet)

ad_entities_per_ad_account: ExpectationGeneratorType = functools.partial(entities_per_ad_account, Entity.Ad)

ad_creative_entities_per_ad_account: ExpectationGeneratorType = functools.partial(
    entities_per_ad_account, Entity.AdCreative
)

ad_video_entities_per_ad_account: ExpectationGeneratorType = functools.partial(entities_per_ad_account, Entity.AdVideo)

custom_audience_entities_per_ad_account: ExpectationGeneratorType = functools.partial(
    entities_per_ad_account, Entity.CustomAudience
)

page_post_entities_per_page: ExpectationGeneratorType = functools.partial(entities_per_page, Entity.PagePost)

comment_entities_per_page_post: ExpectationGeneratorType = functools.partial(entities_per_page_post, Entity.Comment)
