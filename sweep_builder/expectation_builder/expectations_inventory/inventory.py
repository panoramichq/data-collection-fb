"""
This file contains a mapping of normative report type labels to entities that
require these reports and to report task implementations effectively fulfilling
the normative report requirement.
"""
import functools
from typing import Dict, List

from config import jobs as jobs_config
from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from sweep_builder.expectation_builder.expectations_inventory.metrics.breakdowns import (
    day_metrics_per_ads_under_ad_account,
    day_metrics_per_adsets_under_ad_account,
    day_metrics_per_campaigns_under_ad_account,
)
from sweep_builder.expectation_builder.expectations_inventory.page import pages_per_scope
from sweep_builder.types import ExpectationGeneratorType

from sweep_builder.expectation_builder.expectations_inventory.adaccount import ad_accounts_per_scope
from sweep_builder.expectation_builder.expectations_inventory.entities import (
    ad_creative_entities_per_ad_account,
    ad_entities_per_ad_account,
    ad_video_entities_per_ad_account,
    adset_entities_per_ad_account,
    campaign_entities_per_ad_account,
    custom_audience_entities_per_ad_account,
    ad_account_entity,
    page_entity,
    page_post_entities_per_page,
    comment_entities_per_page_post,
    page_video_entities_per_page,
    page_post_promotable_entities_per_page,
)

from sweep_builder.expectation_builder.expectations_inventory.metrics import lifetime

# map of source / trigger entity type to
# a list of generator functions each of which, given RealityClaim instance
# generate one or more ExpectationClaim objects
entity_expectation_generator_map: Dict[str, List[ExpectationGeneratorType]] = {
    Entity.Scope: list(
        filter(
            None,
            [
                None if jobs_config.AD_ACCOUNT_IMPORT_DISABLED else ad_accounts_per_scope,
                None if jobs_config.PAGE_IMPORT_DISABLED else pages_per_scope,
            ],
        )
    ),
    Entity.AdAccount: list(
        filter(
            None,
            [
                # Entities
                None if jobs_config.ENTITY_AA_DISABLED else ad_account_entity,
                None if jobs_config.ENTITY_C_DISABLED else campaign_entities_per_ad_account,
                None if jobs_config.ENTITY_AS_DISABLED else adset_entities_per_ad_account,
                None if jobs_config.ENTITY_A_DISABLED else ad_entities_per_ad_account,
                None if jobs_config.ENTITY_AC_DISABLED else ad_creative_entities_per_ad_account,
                None if jobs_config.ENTITY_AV_DISABLED else ad_video_entities_per_ad_account,
                None if jobs_config.ENTITY_CA_DISABLED else custom_audience_entities_per_ad_account,
                # Insights
                functools.partial(
                    day_metrics_per_ads_under_ad_account,
                    list(
                        filter(
                            None,
                            [
                                None if jobs_config.INSIGHTS_DAY_A_DISABLED else ReportType.day,
                                None if jobs_config.INSIGHTS_HOUR_A_DISABLED else ReportType.day_hour,
                                None if jobs_config.INSIGHTS_AGE_GENDER_A_DISABLED else ReportType.day_age_gender,
                                None if jobs_config.INSIGHTS_DMA_A_DISABLED else ReportType.day_dma,
                                None if jobs_config.INSIGHTS_PLATFORM_A_DISABLED else ReportType.day_platform,
                                None if jobs_config.INSIGHTS_LIFETIME_A_DISABLED else ReportType.lifetime,
                            ],
                        )
                    ),
                ),
                functools.partial(
                    day_metrics_per_adsets_under_ad_account,
                    list(filter(None, [None if jobs_config.INSIGHTS_LIFETIME_AS_DISABLED else ReportType.lifetime])),
                ),
                functools.partial(
                    day_metrics_per_campaigns_under_ad_account,
                    list(filter(None, [None if jobs_config.INSIGHTS_LIFETIME_AS_DISABLED else ReportType.lifetime])),
                ),
            ],
        )
    ),
    Entity.Page: list(
        filter(
            None,
            [
                None if jobs_config.ENTITY_P_DISABLED else page_entity,
                None if jobs_config.ENTITY_PP_DISABLED else page_post_entities_per_page,
                None if jobs_config.ENTITY_PP_DISABLED else page_post_promotable_entities_per_page,
                None if jobs_config.ENTITY_PV_DISABLED else page_video_entities_per_page,
                None if jobs_config.INSIGHTS_LIFETIME_P_DISABLED else lifetime.lifetime_metrics_per_page,
            ],
        )
    ),
    Entity.PagePost: list(
        filter(
            None,
            [
                None if jobs_config.ENTITY_CM_DISABLED else comment_entities_per_page_post,
                None if jobs_config.INSIGHTS_LIFETIME_PP_DISABLED else lifetime.lifetime_metrics_per_page_post,
            ],
        )
    ),
    Entity.PageVideo: list(
        filter(None, [None if jobs_config.INSIGHTS_LIFETIME_PV_DISABLED else lifetime.lifetime_metrics_per_page_video])
    ),
}

# mental note:
# entities per AA data collection hook is as "normative" task on AA,
# not an "effective" task under entity.
# At some point it may be meaningful to have a normative "entity" job on each
# entity level too / instead (where these jobs become "effective" alternatives there)
