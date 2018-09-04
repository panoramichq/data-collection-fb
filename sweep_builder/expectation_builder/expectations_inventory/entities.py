import functools

from typing import Generator

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.enums.reporttype import ReportType
from common.enums.entity import Entity


def entities_per_ad_account(entity_type, reality_claim):
    # type: (str, RealityClaim) -> Generator[ExpectationClaim]
    """
    Generates "fetch EntityType entities metadata per given AA" job call sig

    :param str entity_type: One of Entity enum values
    :param RealityClaim reality_claim:
    :rtype: Generator[ExpectationClaim]
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
        job_signatures = [
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    report_type=ReportType.entity,
                    report_variant=entity_type
                )
            )
        ]
    )


campaign_entities_per_ad_account = functools.partial(
    entities_per_ad_account,
    Entity.Campaign
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


adset_entities_per_ad_account = functools.partial(
    entities_per_ad_account,
    Entity.AdSet
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


ad_entities_per_ad_account = functools.partial(
    entities_per_ad_account,
    Entity.Ad
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

ad_creative_entities_per_ad_account = functools.partial(
    entities_per_ad_account,
    Entity.AdCreative
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

ad_video_entities_per_ad_account = functools.partial(
    entities_per_ad_account,
    Entity.AdVideo
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


custom_audience_entities_per_ad_account = functools.partial(
    entities_per_ad_account,
    Entity.AdVideo
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

