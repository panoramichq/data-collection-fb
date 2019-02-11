import functools

from datetime import date, timedelta
from typing import Generator

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.tztools import now_in_tz, date_range
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.reality_inferrer.reality import iter_reality_per_ad_account_claim


def day_metrics_per_entity(entity_type, day_breakdown, reality_claim):
    # type: (str, str, RealityClaim) -> Generator[ExpectationClaim]
    """
    Given an instance of Reality Claim that refers to one of sub-AdAccount objects,
    calculate the range of data expectations we are to have against that entity.

    :param str entity_type: One of Entity enum values
    :param str day_breakdown: One of ReportType.ALL_DAY_BREAKDOWNS enum values
    :param RealityClaim reality_claim:
    :rtype: Generator[ExpectationClaim]
    """
    if not reality_claim.timezone:
        # For metrics, reality claim must have timezone.
        return
    assert entity_type in Entity.ALL
    assert day_breakdown in ReportType.ALL_DAY_BREAKDOWNS

    base_normative_data = dict(
        ad_account_id=reality_claim.ad_account_id,
        entity_type=reality_claim.entity_type,
        entity_id=reality_claim.entity_id,
        report_type=day_breakdown,
        report_variant=entity_type,
    )

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

    reality_claim_data = reality_claim.to_dict()

    #  for day in date_range(range_start, range_end):
    normative_job_id = generate_id(
        range_start=range_start,
        range_end=range_end,
        **base_normative_data
    )
    yield ExpectationClaim(
        reality_claim_data,
        job_signatures=[JobSignature.bind(normative_job_id)]
    )


def _determine_active_date_range_for_claim(reality_claim):

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


def day_metrics_per_entities_under_ad_account(entity_type, day_breakdown, reality_claim):
    # type: (str, str, RealityClaim) -> Generator[ExpectationClaim]
    """
    Given an instance of Reality Claim that refers AdAccount object,
    calculate the range of data expectations we are to have against
    entity types specified by entity_type variable that are children of the
    this AdAccount.

    Note the difference from day_metrics_per_entity Here we focus entirely on
    one Entity

    :param str entity_type: One of Entity enum values
    :param str day_breakdown: One of ReportType.ALL_DAY_BREAKDOWNS enum values
    :param RealityClaim reality_claim:
    :rtype: Generator[ExpectationClaim]
    """
    if not reality_claim.timezone:
        # For metrics, reality claim must have timezone.
        return
    assert entity_type in Entity.ALL
    assert entity_type is not Entity.AdAccount
    assert day_breakdown in ReportType.ALL_DAY_BREAKDOWNS

    base_normative_data = dict(
        ad_account_id=reality_claim.ad_account_id,
        report_type=day_breakdown,
        report_variant=entity_type
    )

    reality_claim_data = reality_claim.to_dict()
    _days_cache = set()

    for child_reality_claim in iter_reality_per_ad_account_claim(reality_claim, entity_types=[entity_type]):

        range_start, range_end = _determine_active_date_range_for_claim(child_reality_claim)
        for day in date_range(range_start, range_end):

            if day not in _days_cache:
                _days_cache.add(day)

                yield ExpectationClaim(
                    reality_claim_data,
                    job_signatures = [
                        JobSignature.bind(
                            generate_id(
                                range_start=day,
                                **base_normative_data
                            )
                        )
                    ]
                )


# per entity permutation (still need per report type)

_day_metrics_per_campaign = functools.partial(
    day_metrics_per_entity,
    Entity.Campaign
)


_day_metrics_per_adset = functools.partial(
    day_metrics_per_entity,
    Entity.AdSet
)


_day_metrics_per_ad = functools.partial(
    day_metrics_per_entity,
    Entity.Ad
)

# per campaign generators

day_metrics_per_campaign = functools.partial(
    _day_metrics_per_campaign,
    ReportType.day,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]
#
hour_metrics_per_campaign = functools.partial(
    _day_metrics_per_campaign,
    ReportType.day_hour,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

hour_metrics_per_adset_per_entity = functools.partial(
    _day_metrics_per_adset,
    ReportType.day_hour,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

day_metrics_per_ad_per_entity = functools.partial(
    _day_metrics_per_ad,
    ReportType.day,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

hour_metrics_per_ad_per_entity = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_hour,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

day_age_gender_metrics_per_ad_per_entity = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_age_gender,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

day_dma_metrics_per_ad_per_entity = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_dma,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

day_platform_metrics_per_ad_per_entity = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_platform,
)  # type: (RealityClaim) -> Generator[ExpectationClaim]

# per-parent generators

_day_metrics_per_campaign_per_parent = functools.partial(
    day_metrics_per_entities_under_ad_account,
    Entity.Campaign
)


_day_metrics_per_adset_per_parent = functools.partial(
    day_metrics_per_entities_under_ad_account,
    Entity.AdSet
)


_day_metrics_per_ad_per_parent = functools.partial(
    day_metrics_per_entities_under_ad_account,
    Entity.Ad
)



# Per C, per report type

hour_metrics_per_campaign_per_parent = functools.partial(
    _day_metrics_per_campaign_per_parent,
    ReportType.day_hour
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


hour_metrics_per_adset_per_parent = functools.partial(
    _day_metrics_per_adset_per_parent,
    ReportType.day_hour
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


hour_metrics_per_ad_per_parent = functools.partial(
    _day_metrics_per_ad_per_parent,
    ReportType.day_hour
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


# per Ad, day and sub-day breakdowns

day_metrics_per_ad_per_parent = functools.partial(
    _day_metrics_per_ad_per_parent,
    ReportType.day
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_age_gender_metrics_per_ad_per_parent = functools.partial(
    _day_metrics_per_ad_per_parent,
    ReportType.day_age_gender
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_dma_metrics_per_ad_per_parent = functools.partial(
    _day_metrics_per_ad_per_parent,
    ReportType.day_dma
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_platform_metrics_per_ad_per_parent = functools.partial(
    _day_metrics_per_ad_per_parent,
    ReportType.day_platform
)  # type: (RealityClaim) -> Generator[ExpectationClaim]
