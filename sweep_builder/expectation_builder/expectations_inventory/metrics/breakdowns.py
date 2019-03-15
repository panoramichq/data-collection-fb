import functools

from datetime import date, timedelta
from typing import Generator, List, Tuple

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.tztools import now_in_tz, date_range
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.reality_inferrer.reality import iter_reality_per_ad_account_claim
from sweep_builder.types import ExpectationGeneratorType


def lifecycle_metrics_per_entity(
    entity_type: str,
    day_breakdown: str,
    reality_claim: RealityClaim,
) -> Generator[ExpectationClaim, None, None]:
    """
    Create one expectation for whole lifecycle of entity.

    :param entity_type: Type of entity we are reporting on (report level).
    :param day_breakdown: Type of breakdown used in report.
    :param reality_claim: Base reality claim
    """
    if not reality_claim.timezone:
        # For metrics, reality claim must have timezone.
        return
    assert entity_type in Entity.AA_SCOPED
    assert day_breakdown in ReportType.ALL_DAY_BREAKDOWNS

    range_start, range_end = _determine_active_date_range_for_claim(reality_claim)
    # Temporarily go back max 20 days
    range_start = max(range_start, now_in_tz(reality_claim.timezone).date() - timedelta(days=20))
    if range_start > range_end:
        return

    yield ExpectationClaim(
        reality_claim.to_dict(),
        job_signatures=[
            JobSignature.bind(
                generate_id(
                    ad_account_id=reality_claim.ad_account_id,
                    entity_id=reality_claim.entity_id,
                    entity_type=reality_claim.entity_type,
                    report_type=day_breakdown,
                    report_variant=entity_type,
                    range_end=range_end,
                    range_start=range_start,
                )
            ),
        ]
    )


def daily_metrics_per_entity(
    entity_type: str,
    day_breakdown: str,
    reality_claim: RealityClaim,
) -> Generator[ExpectationClaim, None, None]:
    """
    Create expectations for every day in lifecycle of entity.

    :param str entity_type: Type of entity we are reporting on (report level).
    :param str day_breakdown: Type of breakdown used in report.
    :param reality_claim: Base reality claim
    """
    if not reality_claim.timezone:
        # For metrics, reality claim must have timezone.
        return
    assert entity_type in Entity.ALL
    assert day_breakdown in ReportType.ALL_DAY_BREAKDOWNS

    range_start, range_end = _determine_active_date_range_for_claim(reality_claim)
    # Temporarily go back max 20 days
    range_start = max(range_start, now_in_tz(reality_claim.timezone).date() - timedelta(days=20))
    if range_start > range_end:
        return

    for day in date_range(range_start, range_end):
        yield ExpectationClaim(
            reality_claim.to_dict(),
            job_signatures=[
                JobSignature.bind(
                    generate_id(
                        ad_account_id=reality_claim.ad_account_id,
                        entity_id=reality_claim.entity_id,
                        entity_type=reality_claim.entity_type,
                        report_type=day_breakdown,
                        report_variant=entity_type,
                        range_start=day,
                    )
                ),
            ]
        )


def _determine_active_date_range_for_claim(reality_claim: RealityClaim) -> Tuple[date, date]:
    range_start = reality_claim.bol
    # expected to be stored in AA timezone
    if range_start is None:
        # this is odd.. seems we could not infer the beginning of life for this entity
        # it's possible our entity sweeper did not get BOL/EOL for it yet.
        # let's look 30 days back
        # TODO: revisit this default 30 days back thing
        # We don't care about timezone here
        range_start = date.today() + timedelta(days=-30)
    else:
        range_start = range_start.date()

    # this is *inclusive* end. Expect data on this day
    # expected to be stored in AA timezone
    range_end = reality_claim.eol
    if range_end is None:
        # It's normal NOT to have EOL for active campaigns where null EOL means "now"
        # Here we very much care about timezone.
        range_end = now_in_tz(reality_claim.timezone).date()
    else:
        range_end = range_end.date()

    return range_start, range_end


def day_metrics_per_entities_under_ad_account(
    entity_type: str,
    report_types: List[str],
    reality_claim: RealityClaim,
) -> Generator[ExpectationClaim, None, None]:
    """
    Given an instance of Reality Claim that refers AdAccount object,
    calculate the range of data expectations we are to have against
    entity types specified by entity_type variable that are children of the
    this AdAccount.

    Note the difference from day_metrics_per_entity Here we focus entirely on
    one Entity

    :param entity_type: One of Entity enum values
    :param report_types: One of ReportType.ALL_DAY_BREAKDOWNS enum values
    :param reality_claim: Base reality claim
    """
    if not report_types or not reality_claim.timezone:
        return

    reality_claim_data = reality_claim.to_dict()
    _days_cache = set()

    for child_reality_claim in iter_reality_per_ad_account_claim(reality_claim, entity_types=[entity_type]):

        range_start, range_end = _determine_active_date_range_for_claim(child_reality_claim)
        for day in date_range(range_start, range_end):

            if day not in _days_cache:
                _days_cache.add(day)

                for report_type in report_types:
                    yield ExpectationClaim(
                        reality_claim_data,
                        job_signatures=[
                            JobSignature.bind(
                                generate_id(
                                    range_start=day,
                                    report_type=report_type,
                                    ad_account_id=reality_claim.ad_account_id,
                                    report_variant=entity_type,
                                )
                            )
                        ]
                    )


# per entity permutation (still need per report type)

_lifecycle_metrics_per_ad: ExpectationGeneratorType = functools.partial(lifecycle_metrics_per_entity, Entity.Ad)

# per entity generators

day_metrics_per_campaign_per_entity: ExpectationGeneratorType = functools.partial(
    lifecycle_metrics_per_entity,
    Entity.Campaign,
    ReportType.day,
)

hour_metrics_per_campaign_per_entity: ExpectationGeneratorType = functools.partial(
    lifecycle_metrics_per_entity,
    Entity.Campaign,
    ReportType.day_hour,
)

hour_metrics_per_adset_per_entity: ExpectationGeneratorType = functools.partial(
    lifecycle_metrics_per_entity,
    Entity.AdSet,
    ReportType.day_hour,
)

day_metrics_per_ad_per_entity: ExpectationGeneratorType = functools.partial(
    _lifecycle_metrics_per_ad,
    ReportType.day,
)

hour_metrics_per_ad_per_entity: ExpectationGeneratorType = functools.partial(
    _lifecycle_metrics_per_ad,
    ReportType.day_hour,
)

day_age_gender_metrics_per_ad_per_entity: ExpectationGeneratorType = functools.partial(
    _lifecycle_metrics_per_ad,
    ReportType.day_age_gender,
)

day_platform_metrics_per_ad_per_entity: ExpectationGeneratorType = functools.partial(
    _lifecycle_metrics_per_ad,
    ReportType.day_platform,
)

day_dma_metrics_per_ad_per_entity: ExpectationGeneratorType = functools.partial(
    _lifecycle_metrics_per_ad,
    ReportType.day_dma,
)

# Per C, per report type

hour_metrics_per_campaign_per_parent: ExpectationGeneratorType = functools.partial(
    day_metrics_per_entities_under_ad_account,
    Entity.Campaign,
    [ReportType.day_hour],
)

# Per AS, per report type

hour_metrics_per_adset_per_parent: ExpectationGeneratorType = functools.partial(
    day_metrics_per_entities_under_ad_account,
    Entity.AdSet,
    [ReportType.day_hour],
)

# per Ad, day and sub-day breakdowns

metrics_per_ad_per_ad_account: ExpectationGeneratorType = functools.partial(
    day_metrics_per_entities_under_ad_account,
    Entity.Ad,
)
