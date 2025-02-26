import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Generator, List, Tuple, Dict, Set

from common.enums.entity import Entity
from common.id_tools import generate_id
from common.job_signature import JobSignature
from common.tztools import now_in_tz, date_range
from sweep_builder.data_containers.entity_node import EntityNode
from sweep_builder.data_containers.expectation_claim import ExpectationClaim
from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.reality_inferrer.reality import iter_reality_per_ad_account_claim

logger = logging.getLogger(__name__)


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


def day_metrics_per_ads_under_ad_account(
    report_types: List[str], reality_claim: RealityClaim
) -> Generator[ExpectationClaim, None, None]:
    """Generate ad-account level expectation claims for every day."""
    if not report_types or not reality_claim.timezone:
        return

    active_adset_ids_by_day: Dict[date, Set[str]] = defaultdict(set)
    adset_campaigns: Dict[str, str] = {}

    # TODO: Remove once all entities have parent ids
    # Divide tasks only if parent levels are defined for all ads
    is_dividing_possible = True

    # we are stopping task breakdown at lowest "parent" possible - AdSet.
    # Don't go "Ad" - too inefficient for US at this point as Celery task overhead and
    # scheduling will cost us more than collection and tracking of individual ad level data task.
    for child_claim in iter_reality_per_ad_account_claim(reality_claim, entity_types=[Entity.Ad]):
        is_dividing_possible = is_dividing_possible and child_claim.all_parent_ids_set
        adset_campaigns[child_claim.adset_id] = child_claim.campaign_id
        range_start, range_end = _determine_active_date_range_for_claim(child_claim)
        for day in date_range(range_start, range_end):
            active_adset_ids_by_day[day].add(child_claim.adset_id)

    logger.warning(
        f'[dividing-possible] Ad Account {reality_claim.ad_account_id} Dividing possible: {is_dividing_possible}'
    )

    # We generate a claim of existence of child records for each day we see signs of
    # existence / activity for any child of ad account.
    # Note that here we do NOT generate separate Job IDs
    # Thus, we yield claims that are effectively combination of
    # existence day, report type (indicating what kind of record migght exist)
    # Obviously this approach works only for metrics report types that have day-based dimension.
    for (day, active_adset_ids) in active_adset_ids_by_day.items():

        ad_account_node = EntityNode(reality_claim.entity_id, reality_claim.entity_type)
        for adset_id in active_adset_ids:
            campaign_id = adset_campaigns[adset_id]
            ad_account_node.add_node(EntityNode(adset_id, Entity.AdSet), path=(campaign_id,))

        for report_type in report_types:
            yield ExpectationClaim(
                reality_claim.entity_id,
                reality_claim.entity_type,
                report_type,
                Entity.Ad,
                JobSignature(
                    generate_id(
                        ad_account_id=reality_claim.ad_account_id,
                        range_start=day,
                        report_type=report_type,
                        report_variant=Entity.Ad,
                    )
                ),
                ad_account_id=reality_claim.ad_account_id,
                timezone=reality_claim.timezone,
                entity_hierarchy=ad_account_node if is_dividing_possible else None,
                range_start=day,
            )
