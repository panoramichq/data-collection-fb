from facebook_business.adobjects.insightsresult import InsightsResult

from common.enums.entity import Entity
from facebook_business.adobjects.adsinsights import AdsInsights
from common.enums.reporttype import ReportType
from oozer.common.enum import ReportEntityApiKind

ENUM_LEVEL_MAP = {
    Entity.AdAccount: AdsInsights.Level.account,
    Entity.Campaign: AdsInsights.Level.campaign,
    Entity.AdSet: AdsInsights.Level.adset,
    Entity.Ad: AdsInsights.Level.ad,
}

REPORT_TYPE_FB_BREAKDOWN_ENUM = {
    ReportType.day: None,
    ReportType.day_age_gender: [AdsInsights.Breakdowns.age, AdsInsights.Breakdowns.gender],
    ReportType.day_dma: [AdsInsights.Breakdowns.dma],
    ReportType.day_region: [AdsInsights.Breakdowns.region],
    ReportType.day_country: [AdsInsights.Breakdowns.country],
    ReportType.day_hour: [AdsInsights.Breakdowns.hourly_stats_aggregated_by_advertiser_time_zone],
    ReportType.day_platform: [AdsInsights.Breakdowns.publisher_platform, AdsInsights.Breakdowns.platform_position],
}

DEFAULT_REPORT_FIELDS = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.ad_id,
    # Non-unique
    # Essential
    AdsInsights.Field.spend,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.actions,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p95_watched_actions,
    AdsInsights.Field.video_p100_watched_actions,
    AdsInsights.Field.video_30_sec_watched_actions,
    # Good to have
    AdsInsights.Field.cost_per_action_type,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cpp,
    AdsInsights.Field.ctr,
    AdsInsights.Field.cpc,
    # AdsInsights.Field.relevance_score, Why? => https://operam.atlassian.net/browse/PROD-4362
    AdsInsights.Field.video_avg_time_watched_actions,
    # Not sure
    AdsInsights.Field.action_values,
    # 'inline_link_clicks',
    # 'inline_post_engagement',
    # 'social_clicks',
    # 'social_impressions',
    # 'social_reach',
    # 'social_spend',
    #
    # 'action_values',
    # 'buying_type',
    # 'call_to_action_clicks',
    # 'cost_per_10_sec_video_view',
    # 'cost_per_estimated_ad_recallers',
    # 'cost_per_inline_link_click',
    # 'cost_per_inline_post_engagement',
    # 'cost_per_total_action',
    AdsInsights.Field.estimated_ad_recall_rate,
    AdsInsights.Field.estimated_ad_recallers,
    AdsInsights.Field.cost_per_estimated_ad_recallers,
    # 'estimated_ad_recallers',
    # 'total_action_value',
    # 'website_ctr',
    # Unique
    # Essential
    AdsInsights.Field.unique_actions,
    AdsInsights.Field.video_thruplay_watched_actions,
    AdsInsights.Field.reach,
    # Good to have
    AdsInsights.Field.frequency,
    AdsInsights.Field.cost_per_unique_action_type,
    # Not sure
    # 'cost_per_unique_click',
    # 'total_unique_actions',
    #
    # 'unique_clicks',
    # 'unique_ctr',
    # 'unique_link_clicks_ctr',
    # 'unique_social_clicks',
]

# "Default" attribution is 28d Click & 1d View.
DEFAULT_ATTRIBUTION_WINDOWS = [
    AdsInsights.ActionAttributionWindows.value_1d_view,
    AdsInsights.ActionAttributionWindows.value_7d_view,
    AdsInsights.ActionAttributionWindows.value_28d_view,
    AdsInsights.ActionAttributionWindows.value_1d_click,
    AdsInsights.ActionAttributionWindows.value_7d_click,
    AdsInsights.ActionAttributionWindows.value_28d_click,
    AdsInsights.ActionAttributionWindows.value_default,
]

PAGE_METRICS = [
    'page_content_activity_by_action_type_unique',
    'page_content_activity_by_age_gender_unique',
    'page_content_activity_by_city_unique',
    'page_content_activity_by_country_unique',
    'page_content_activity_by_locale_unique',
    'page_content_activity',
    'page_content_activity_by_action_type',
    'page_content_activity_unique',
    'page_impressions',
    'page_impressions_unique',
    'page_impressions_paid',
    'page_impressions_paid_unique',
    'page_impressions_organic',
    'page_impressions_organic_unique',
    'page_impressions_viral',
    'page_impressions_viral_unique',
    'page_impressions_nonviral',
    'page_impressions_nonviral_unique',
    'page_impressions_by_story_type',
    'page_impressions_by_story_type_unique',
    'page_impressions_by_city_unique',
    'page_impressions_by_country_unique',
    'page_impressions_by_locale_unique',
    'page_impressions_by_age_gender_unique',
    'page_impressions_frequency_distribution',
    'page_impressions_viral_frequency_distribution',
    'page_engaged_users',
    'page_post_engagements',
    'page_consumptions',
    'page_consumptions_unique',
    'page_consumptions_by_consumption_type',
    'page_consumptions_by_consumption_type_unique',
    'page_places_checkin_total',
    'page_places_checkin_total_unique',
    'page_places_checkin_mobile',
    'page_places_checkin_mobile_unique',
    'page_places_checkins_by_age_gender',
    'page_places_checkins_by_locale',
    'page_places_checkins_by_country',
    'page_negative_feedback',
    'page_negative_feedback_unique',
    'page_negative_feedback_by_type',
    'page_negative_feedback_by_type_unique',
    'page_positive_feedback_by_type',
    'page_positive_feedback_by_type_unique',
    'page_fans_online',
    'page_fans_online_per_day',
    'page_fan_adds_by_paid_non_paid_unique',
    'page_actions_post_reactions_like_total',
    'page_actions_post_reactions_love_total',
    'page_actions_post_reactions_wow_total',
    'page_actions_post_reactions_haha_total',
    'page_actions_post_reactions_sorry_total',
    'page_actions_post_reactions_anger_total',
    'page_actions_post_reactions_total',
    'page_total_actions',
    'page_cta_clicks_logged_in_total',
    'page_cta_clicks_logged_in_unique',
    'page_cta_clicks_by_site_logged_in_unique',
    'page_cta_clicks_by_age_gender_logged_in_unique',
    'page_cta_clicks_logged_in_by_country_unique',
    'page_cta_clicks_logged_in_by_city_unique',
    'page_call_phone_clicks_logged_in_unique',
    'page_call_phone_clicks_by_age_gender_logged_in_unique',
    'page_call_phone_clicks_logged_in_by_country_unique',
    'page_call_phone_clicks_logged_in_by_city_unique',
    'page_call_phone_clicks_by_site_logged_in_unique',
    'page_get_directions_clicks_logged_in_unique',
    'page_get_directions_clicks_by_age_gender_logged_in_unique',
    'page_get_directions_clicks_logged_in_by_country_unique',
    'page_get_directions_clicks_logged_in_by_city_unique',
    'page_get_directions_clicks_by_site_logged_in_unique',
    'page_website_clicks_logged_in_unique',
    'page_website_clicks_by_age_gender_logged_in_unique',
    'page_website_clicks_logged_in_by_country_unique',
    'page_website_clicks_logged_in_by_city_unique',
    'page_website_clicks_by_site_logged_in_unique',
    'page_fans',
    'page_fans_locale',
    'page_fans_city',
    'page_fans_country',
    'page_fans_gender_age',
    'page_fan_adds',
    'page_fan_adds_unique',
    'page_fans_by_like_source',
    'page_fans_by_like_source_unique',
    'page_fan_removes',
    'page_fan_removes_unique',
    'page_fans_by_unlike_source_unique',
    'page_tab_views_login_top_unique',
    'page_tab_views_login_top',
    'page_tab_views_logout_top',
    'page_views_total',
    'page_views_logout',
    'page_views_logged_in_total',
    'page_views_logged_in_unique',
    'page_views_external_referrals',
    'page_views_by_profile_tab_total',
    'page_views_by_profile_tab_logged_in_unique',
    'page_views_by_internal_referer_logged_in_unique',
    'page_views_by_site_logged_in_unique',
    'page_views_by_age_gender_logged_in_unique',
    'page_views_by_referers_logged_in_unique',
    'page_video_views',
    'page_video_views_paid',
    'page_video_views_organic',
    'page_video_views_by_paid_non_paid',
    'page_video_views_autoplayed',
    'page_video_views_click_to_play',
    'page_video_views_unique',
    'page_video_repeat_views',
    'page_video_complete_views_30s',
    'page_video_complete_views_30s_paid',
    'page_video_complete_views_30s_organic',
    'page_video_complete_views_30s_autoplayed',
    'page_video_complete_views_30s_click_to_play',
    'page_video_complete_views_30s_unique',
    'page_video_complete_views_30s_repeat_views',
    'page_video_views_10s',
    'page_video_views_10s_paid',
    'page_video_views_10s_organic',
    'page_video_views_10s_autoplayed',
    'page_video_views_10s_click_to_play',
    'page_video_views_10s_unique',
    'page_video_views_10s_repeat',
    'page_video_view_time',
    'page_posts_impressions',
    'page_posts_impressions_unique',
    'page_posts_impressions_paid',
    'page_posts_impressions_paid_unique',
    'page_posts_impressions_organic',
    'page_posts_impressions_organic_unique',
    'page_posts_served_impressions_organic_unique',
    'page_posts_impressions_viral',
    'page_posts_impressions_viral_unique',
    'page_posts_impressions_nonviral',
    'page_posts_impressions_nonviral_unique',
    'page_posts_impressions_frequency_distribution',
    'page_daily_video_ad_break_ad_impressions_by_crosspost_status',
    'page_daily_video_ad_break_cpm_by_crosspost_status',
    'page_daily_video_ad_break_earnings_by_crosspost_status',
]

POST_METRICS = [
    'post_activity',
    'post_activity_unique',
    'post_activity_by_action_type',
    'post_activity_by_action_type_unique',
    'post_clicks',
    'post_clicks_unique',
    'post_clicks_by_type',
    'post_clicks_by_type_unique',
    'post_video_complete_views_30s_autoplayed',
    'post_video_complete_views_30s_clicked_to_play',
    'post_video_complete_views_30s_organic',
    'post_video_complete_views_30s_paid',
    'post_video_complete_views_30s_unique',
    'post_impressions',
    'post_impressions_unique',
    'post_impressions_paid',
    'post_impressions_paid_unique',
    'post_impressions_fan',
    'post_impressions_fan_unique',
    'post_impressions_fan_paid',
    'post_impressions_fan_paid_unique',
    'post_impressions_organic',
    'post_impressions_organic_unique',
    'post_impressions_viral',
    'post_impressions_viral_unique',
    'post_impressions_nonviral',
    'post_impressions_nonviral_unique',
    'post_impressions_by_story_type',
    'post_impressions_by_story_type_unique',
    'post_engaged_users',
    'post_negative_feedback',
    'post_negative_feedback_unique',
    'post_negative_feedback_by_type',
    'post_negative_feedback_by_type_unique',
    'post_engaged_fan',
    'post_clicks',
    'post_clicks_unique',
    'post_clicks_by_type',
    'post_clicks_by_type_unique',
    'post_reactions_like_total',
    'post_reactions_love_total',
    'post_reactions_wow_total',
    'post_reactions_haha_total',
    'post_reactions_sorry_total',
    'post_reactions_anger_total',
    'post_reactions_by_type_total',
    'post_video_avg_time_watched',
    'post_video_complete_views_organic',
    'post_video_complete_views_organic_unique',
    'post_video_complete_views_paid',
    'post_video_complete_views_paid_unique',
    'post_video_retention_graph',
    'post_video_retention_graph_clicked_to_play',
    'post_video_retention_graph_autoplayed',
    'post_video_views_organic',
    'post_video_views_organic_unique',
    'post_video_views_paid',
    'post_video_views_paid_unique',
    'post_video_length',
    'post_video_views',
    'post_video_views_unique',
    'post_video_views_autoplayed',
    'post_video_views_clicked_to_play',
    'post_video_views_10s',
    'post_video_views_10s_unique',
    'post_video_views_10s_autoplayed',
    'post_video_views_10s_clicked_to_play',
    'post_video_views_10s_organic',
    'post_video_views_10s_paid',
    'post_video_views_10s_sound_on',
    'post_video_views_sound_on',
    'post_video_view_time',
    'post_video_view_time_organic',
    'post_video_view_time_by_age_bucket_and_gender',
    'post_video_view_time_by_region_id',
    'post_video_views_by_distribution_type',
    'post_video_view_time_by_distribution_type',
    'post_video_view_time_by_country_id',
    'post_video_ad_break_ad_impressions',
    'post_video_ad_break_earnings',
    'post_video_ad_break_ad_cpm',
]

VIDEO_REPORT_METRICS = [
    'total_video_views',
    'total_video_views_unique',
    'total_video_views_autoplayed',
    'total_video_views_clicked_to_play',
    'total_video_views_organic',
    'total_video_views_organic_unique',
    'total_video_views_paid',
    'total_video_views_paid_unique',
    'total_video_views_sound_on',
    'total_video_complete_views',
    'total_video_complete_views_unique',
    'total_video_complete_views_auto_played',
    'total_video_complete_views_clicked_to_play',
    'total_video_complete_views_organic',
    'total_video_complete_views_organic_unique',
    'total_video_complete_views_paid',
    'total_video_complete_views_paid_unique',
    'total_video_10s_views',
    'total_video_10s_views_unique',
    'total_video_10s_views_auto_played',
    'total_video_10s_views_clicked_to_play',
    'total_video_10s_views_organic',
    'total_video_10s_views_paid',
    'total_video_10s_views_sound_on',
    'total_video_retention_graph',
    'total_video_retention_graph_autoplayed',
    'total_video_retention_graph_clicked_to_play',
    'total_video_avg_time_watched',
    'total_video_view_total_time',
    'total_video_view_total_time_organic',
    'total_video_view_total_time_paid',
    'total_video_impressions',
    'total_video_impressions_unique',
    'total_video_impressions_paid_unique',
    'total_video_impressions_paid',
    'total_video_impressions_organic_unique',
    'total_video_impressions_organic',
    'total_video_impressions_viral_unique',
    'total_video_impressions_viral',
    'total_video_impressions_fan_unique',
    'total_video_impressions_fan',
    'total_video_impressions_fan_paid_unique',
    'total_video_impressions_fan_paid',
    'total_video_stories_by_action_type',
    'total_video_reactions_by_type_total',
    'total_video_view_time_by_age_bucket_and_gender',
    'total_video_view_time_by_region_id',
    'total_video_views_by_distribution_type',
    'total_video_view_time_by_distribution_type',
]

ORGANIC_DATA_FIELDS_MAP = {
    ReportEntityApiKind.Page: PAGE_METRICS,
    ReportEntityApiKind.Post: POST_METRICS,
    ReportEntityApiKind.Video: VIDEO_REPORT_METRICS,
}

INSIGHTS_REPORT_FIELDS = [InsightsResult.Field.name, InsightsResult.Field.values]
