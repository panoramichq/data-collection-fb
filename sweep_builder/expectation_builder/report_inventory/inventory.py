"""
This file contains a mapping of normative report type labels to entities that
require these reports and to report task implementations effectively fulfilling
the normative report requirement.
"""
from typing import Dict, List, Optional, Callable, Generator

from common.enums.entity import Entity

from sweep_builder.reality_inferrer.reality_claim import RealityClaim
from sweep_builder.expectation_builder.expectation_claim import ExpectationClaim


# map of source / trigger entity type to
# a list of generator functions each of which, given RealityClaim instance
# generate one or more ExpectationClaim objects
entity_jobs_gens_map = {}  # type: Dict[str, List[(RealityClaim) -> Generator[ExpectationClaim]]]


from .entities import (
    campaign_entities_per_ad_account,
    adset_entities_per_ad_account,
    ad_entities_per_ad_account
)


# mental note:
# entities per AA data collection hook is as "normative" task on AA,
# not an "effective" task under entity.
# At some point it may be meaningful to have a normative "entity" job on each
# entity level too / instead (where these jobs become "effective" alternatives there)
entity_jobs_gens_map[Entity.AdAccount] = [
    campaign_entities_per_ad_account,
    adset_entities_per_ad_account,
    ad_entities_per_ad_account
]


from .metrics import lifetime, breakdowns


entity_jobs_gens_map[Entity.Campaign] = [
    lifetime.lifetime_metrics_per_campaign,
    # breakdowns.day_age_gender_metrics_per_campaign,
    # breakdowns.day_dma_metrics_per_campaign,
    # breakdowns.day_hour_metrics_per_campaign  # not required per our spec
]


entity_jobs_gens_map[Entity.AdSet] = [
    lifetime.lifetime_metrics_per_adset,
    # breakdowns.day_age_gender_metrics_per_adset,
    # breakdowns.day_dma_metrics_per_adset,
    # breakdowns.day_hour_metrics_per_adset  # not required per our spec
]


entity_jobs_gens_map[Entity.Ad] = [
    lifetime.lifetime_metrics_per_ad,
    # breakdowns.day_age_gender_metrics_per_ad,
    # breakdowns.day_dma_metrics_per_ad,
    breakdowns.day_hour_metrics_per_ad
]
