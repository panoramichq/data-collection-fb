import functools

from datetime import date, timedelta
from typing import Generator

from common.facebook.enums.entity import Entity
from common.facebook.enums.reporttype import ReportType
from common.id_tools import generate_id
from common.job_signature import JobSignature
from sweep_builder.expectation_builder.expectation_claim import ExpectationClaim
from sweep_builder.reality_inferrer.reality_claim import RealityClaim
from common.tztools import now_in_tz, date_range


def day_metrics_per_entity(entity_type, day_breakdown, reality_claim):
    # type: (str, str, RealityClaim) -> Generator[ExpectationClaim]
    """
    :param str entity_type: One of Entity enum values
    :param str day_breakdown: One of ReportType.ALL_DAY_BREAKDOWNS enum values
    :param RealityClaim reality_claim:
    :rtype: Generator[ExpectationClaim]
    """

    assert entity_type in Entity.ALL
    assert day_breakdown in ReportType.ALL_DAY_BREAKDOWNS

    base_normative_data = dict(
        ad_account_id=reality_claim.ad_account_id,
        entity_type=entity_type,
        entity_id=reality_claim.entity_id,
        report_type=day_breakdown
    )

    base_effective_data = dict(
        ad_account_id=reality_claim.ad_account_id,
        report_type=day_breakdown,
        report_variant=entity_type
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

    for day in date_range(range_start, range_end):
        normative_job_id = generate_id(
            range_start=day,
            **base_normative_data
        )
        yield ExpectationClaim(
            reality_claim_data,
            job_signatures = [
                # normative job signature
                JobSignature.bind(
                    normative_job_id
                ),
                # possible alternative "effective" job signatures:
                JobSignature.bind(
                    generate_id(
                        range_start=day,
                        **base_effective_data
                    ),
                    normative_job_id=normative_job_id
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


# Per C, per report type

day_age_gender_metrics_per_campaign = functools.partial(
    _day_metrics_per_campaign,
    ReportType.day_age_gender
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_dma_metrics_per_campaign = functools.partial(
    _day_metrics_per_campaign,
    ReportType.day_dma
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_hour_metrics_per_campaign = functools.partial(
    _day_metrics_per_campaign,
    ReportType.day_hour
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


# Per AdSet, per report type

day_age_gender_metrics_per_adset = functools.partial(
    _day_metrics_per_adset,
    ReportType.day_age_gender
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_dma_metrics_per_adset = functools.partial(
    _day_metrics_per_adset,
    ReportType.day_dma
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_hour_metrics_per_adset = functools.partial(
    _day_metrics_per_adset,
    ReportType.day_hour
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


# per Ad, per report type


day_age_gender_metrics_per_ad = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_age_gender
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_dma_metrics_per_ad = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_dma
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_hour_metrics_per_ad = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_hour
)  # type: (RealityClaim) -> Generator[ExpectationClaim]


day_platform_metrics_per_ad = functools.partial(
    _day_metrics_per_ad,
    ReportType.day_platform
)  # type: (RealityClaim) -> Generator[ExpectationClaim]
