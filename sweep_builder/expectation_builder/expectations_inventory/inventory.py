"""
This file contains a mapping of normative report type labels to entities that
require these reports and to report task implementations effectively fulfilling
the normative report requirement.
"""
import functools
from typing import Dict, List, Generator, Callable, Optional

from common.enums.entity import Entity

from sweep_builder.data_containers.reality_claim import RealityClaim
from sweep_builder.data_containers.expectation_claim import ExpectationClaim

from config import jobs as jobs_config

# map of source / trigger entity type to
# a list of generator functions each of which, given RealityClaim instance
# generate one or more ExpectationClaim objects
JobGenerator = Callable[[RealityClaim], Generator[ExpectationClaim, None, None]]

# This is a map of generator getters. Based on reality claim (especially timezone), it returns proper job generators.
entity_expectation_generator_map = {}  # type: Dict[str, Callable[[RealityClaim],List[JobGenerator]]]

from .adaccount import ad_accounts_per_scope, sync_expectations_per_ad_account

entity_expectation_generator_map[Entity.Scope] = lambda reality: list(filter(None, [
    None if jobs_config.AD_ACCOUNT_IMPORT_DISABLED else ad_accounts_per_scope,
]))

from .entities import (
    ad_creative_entities_per_ad_account,
    ad_entities_per_ad_account,
    ad_video_entities_per_ad_account,
    adset_entities_per_ad_account,
    campaign_entities_per_ad_account,
    custom_audience_entities_per_ad_account,
    ad_account_entity
)

from .metrics import lifetime, breakdowns

# mental note:
# entities per AA data collection hook is as "normative" task on AA,
# not an "effective" task under entity.
# At some point it may be meaningful to have a normative "entity" job on each
# entity level too / instead (where these jobs become "effective" alternatives there)
def _get_ad_account_job_generators(reality: RealityClaim) -> List[JobGenerator]:
    job_generators = [
        sync_expectations_per_ad_account,
        None if jobs_config.ENTITY_AA_DISABLED else ad_account_entity,
        None if jobs_config.ENTITY_C_DISABLED else campaign_entities_per_ad_account,
        None if jobs_config.ENTITY_AS_DISABLED else adset_entities_per_ad_account,
        None if jobs_config.ENTITY_A_DISABLED else ad_entities_per_ad_account,
        None if jobs_config.ENTITY_AC_DISABLED else ad_creative_entities_per_ad_account,
        None if jobs_config.ENTITY_AV_DISABLED else ad_video_entities_per_ad_account,
        None if jobs_config.ENTITY_CA_DISABLED else custom_audience_entities_per_ad_account,
    ]
    if reality.timezone:
        # Generate metric jobs only if ad account has timezone set.
        job_generators += [
            None if jobs_config.INSIGHTS_HOUR_C_DISABLED else breakdowns.hour_metrics_per_campaign_per_parent,
            None if jobs_config.INSIGHTS_HOUR_AS_DISABLED else breakdowns.hour_metrics_per_adset_per_parent,
            None if jobs_config.INSIGHTS_DAY_A_DISABLED else breakdowns.day_metrics_per_ad_per_parent,
            None if jobs_config.INSIGHTS_HOUR_A_DISABLED else breakdowns.hour_metrics_per_ad_per_parent,
            None if jobs_config.INSIGHTS_AGE_GENDER_A_DISABLED else breakdowns.day_age_gender_metrics_per_ad_per_parent,
            None if jobs_config.INSIGHTS_DMA_A_DISABLED else breakdowns.day_dma_metrics_per_ad_per_parent,
            None if jobs_config.INSIGHTS_PLATFORM_A_DISABLED else breakdowns.day_platform_metrics_per_ad_per_parent,
        ]
    return list(filter(None, job_generators))


entity_expectation_generator_map[Entity.AdAccount] = _get_ad_account_job_generators


def _get_metric_job_generators(generators: List[Optional[JobGenerator]], reality: RealityClaim) -> List[JobGenerator]:
    if reality.timezone:
        # Generate metric jobs only if ad account has timezone set.
        return list(filter(None, generators))
    else:
        return []


entity_expectation_generator_map[Entity.Campaign] = functools.partial(_get_metric_job_generators, [
    None if jobs_config.INSIGHTS_LIFETIME_C_DISABLED else lifetime.lifetime_metrics_per_campaign,
])

entity_expectation_generator_map[Entity.AdSet] = functools.partial(_get_metric_job_generators, [
    None if jobs_config.INSIGHTS_LIFETIME_AS_DISABLED else lifetime.lifetime_metrics_per_adset,
])

entity_expectation_generator_map[Entity.Ad] = functools.partial(_get_metric_job_generators, [
    None if jobs_config.INSIGHTS_LIFETIME_A_DISABLED else lifetime.lifetime_metrics_per_ad,
])
