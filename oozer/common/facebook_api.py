from typing import List, Type, Any, Dict, Tuple, Optional

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.comment import Comment
from facebook_business.api import FacebookAdsApi, FacebookSession
from facebook_business.adobjects import abstractcrudobject
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.pagepost import PagePost

from common.enums.failure_bucket import FailureBucket
from oozer.common.enum import to_fb_model, ExternalPlatformJobStatus
from oozer.common.facebook_fields import collapse_fields_children


class PlatformApiContext:
    """
    A simple wrapper for Facebook SDK, using local API sessions as not to
    pollute the the global default API session with initialization
    """

    token: str = None
    api: FacebookAdsApi = None

    def __init__(self, token: str):
        self.token = token

    def __enter__(self) -> 'PlatformApiContext':
        self.api = FacebookAdsApi(FacebookSession(access_token=self.token))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        We do not need to do anything specific yet
        """
        pass

    def to_fb_model(self, entity_id: str, entity_type: str):
        """
        Like stand-alone to_fb_model but removes the need to pass in API
        instance manually
        """
        return to_fb_model(entity_id, entity_type, self.api)


class FacebookApiErrorInspector:
    """
    A vehicle to store the information on distinct *types* of errors that FB
    cna throw at us and we're interested in them
    """

    ERROR_CODE_MAP: Dict[Tuple[int, int], Tuple[int, int]] = {
        # Application request limit reached
        (4, None): (ExternalPlatformJobStatus.ApplicationThrottlingError, FailureBucket.ApplicationThrottling),
        # User request limit reached
        (17, None): (ExternalPlatformJobStatus.UserThrottlingError, FailureBucket.UserThrottling),
        # User request limit reached
        (17, 2446079): (ExternalPlatformJobStatus.UserThrottlingError, FailureBucket.UserThrottling),
        # AdAccount request limit reached
        (613, 1487742): (ExternalPlatformJobStatus.AdAccountThrottlingError, FailureBucket.AdAccountThrottling),
        # Too big a report
        (100, 1487534): (ExternalPlatformJobStatus.TooMuchData, FailureBucket.TooLarge),
        # Object does not exist, cannot be loaded due to missing permissions, or does not support this operation
        (100, 13): (ExternalPlatformJobStatus.InaccessibleObject, FailureBucket.InaccessibleObject),
        (100, 33): (ExternalPlatformJobStatus.InaccessibleObject, FailureBucket.InaccessibleObject),
    }

    ERROR_MESSAGE_MAP = {
        (1, "Please reduce the amount of data you're asking for, then retry your request"): (
            ExternalPlatformJobStatus.TooMuchData,
            FailureBucket.TooLarge,
        )
    }

    _exception: FacebookRequestError

    def __init__(self, exception: FacebookRequestError):
        """Store the exception we want to test."""
        self._exception = exception

    def get_status_and_bucket(self) -> Optional[Tuple[int, int]]:
        """Extract status and bucket from inspected exception."""
        code = self._exception.api_error_code()
        subcode = self._exception.api_error_subcode()
        error_message = self._exception.api_error_message()

        status_bucket = self.ERROR_CODE_MAP.get((code, subcode))
        if status_bucket is None:
            status_bucket = self.ERROR_MESSAGE_MAP.get((code, error_message))

        if status_bucket is None:
            status_bucket = ExternalPlatformJobStatus.GenericPlatformError, FailureBucket.Other

        return status_bucket


_default_fields_map = {
    AdAccount: collapse_fields_children(
        [
            'id',
            'account_id',
            'name',
            'account_status',
            'amount_spent',
            'attribution_spec',
            'can_create_brand_lift_study',
            'capabilities',
            'currency',
            'end_advertiser',
            'end_advertiser_name',
            'owner',
            'rf_spec',
            'spend_cap',
            'timezone_id',
            'timezone_name',
            'timezone_offset_hours_utc',
        ]
    ),
    Campaign: collapse_fields_children(
        [
            'account_id',
            'adlabels',
            # 'boosted_object_id',
            # 'brand_lift_studies',
            # 'budget_rebalance_flag',
            'buying_type',
            'can_create_brand_lift_study',
            'can_use_spend_cap',
            # 'configured_status',  # same as `status`
            'created_time',
            'effective_status',
            # 'execution_options',
            'id',
            # 'kpi_custom_conversion_id',
            # 'kpi_type',
            'name',
            'objective',
            # 'promoted_object',
            # 'recommendations',
            # 'source_campaign',
            # 'source_campaign_id',
            'spend_cap',
            'start_time',
            'status',
            'stop_time',
            'updated_time',
        ]
    ),
    AdSet: collapse_fields_children(
        [
            'account_id',
            # 'adlabels',
            # 'adset_schedule',
            # 'attribution_spec',
            'bid_amount',
            'bid_info',
            'billing_event',
            'budget_remaining',  # TODO: Temp enable. Migrate Console away from use.
            # 'campaign',
            'campaign_id',
            # 'campaign_spec',
            # 'configured_status',
            'created_time',
            # 'creative_sequence',
            'daily_budget',
            # 'daily_imp',
            # 'destination_type',
            'effective_status',
            'end_time',
            # 'execution_options',
            # 'frequency_control_specs',
            'id',
            # 'instagram_actor_id',
            # ' is_autobid',  # Deprecated in v3.0, replaced with `bid_strategy`
            # 'is_average_price_pacing',  # Deprecated in v3.0
            'bid_strategy',
            'lifetime_budget',
            # 'lifetime_imps',
            'name',
            'optimization_goal',
            # 'pacing_type',
            # 'promoted_object',  # TODO: need Page ID and more from here.
            # 'recommendations',
            # 'recurring_budget_semantics',
            # 'redownload',
            # 'rf_prediction_id',
            # 'rtb_flag',
            # 'source_adset',
            # 'source_adset_id',
            'start_time',
            'status',
            # Yes we need it, but beware! https://drive.google.com/drive/folders/1R01e7WiilzKDPYTKpVnUXQQyIUldFmCK
            'targeting',
            # 'time_based_ad_rotation_id_blocks',
            # 'time_based_ad_rotation_intervals',
            'updated_time',
            # 'use_new_app_click'
        ]
    ),
    Ad: collapse_fields_children(
        [
            'account_id',
            # 'ad_review_feedback', <----- !!!!!!!!!
            # 'adlabels', # <----- !!!!!!!!!
            # 'adset', <----- !!!!!!!!!
            'adset_id',
            # 'adset_spec', <----- !!!!!!!!!
            # 'bid_amount',
            # 'bid_info',
            # 'bid_type',
            # 'campaign', <----- !!!!!!!!!
            'campaign_id',
            # 'configured_status',
            # 'conversion_specs',
            'created_time',
            # Asking for "name" for backwards-compatibility with old Console code
            # asking for effective_*_story_id because we can.
            ('creative', ['id', 'effective_instagram_story_id', 'effective_object_story_id', 'name']),
            # 'creative', # 'id' field is communicated by default
            # 'date_format',
            # 'display_sequence',
            'effective_status',
            # 'execution_options',
            # 'filename',
            'id',
            'last_updated_by_app_id',
            'name',
            # 'recommendations', <----- !!!!!!!!!
            # 'redownload', <----- not a field. it's a flag
            # 'source_ad',
            'source_ad_id',
            'status',
            'tracking_specs',  # <----- !!!!!!!!!
            'updated_time',
        ]
    ),
    AdCreative: collapse_fields_children(
        [
            'id',
            'account_id',
            'actor_id',
            'adlabels',
            'applink_treatment',
            'asset_feed_spec',
            'body',
            'branded_content_sponsor_page_id',
            'call_to_action_type',
            'effective_instagram_story_id',
            'effective_object_story_id',
            'image_crops',
            'image_hash',
            'image_url',
            'instagram_actor_id',
            'instagram_permalink_url',
            'instagram_story_id',
            'link_og_id',
            'link_url',
            'name',
            'object_id',
            'object_story_id',
            'object_story_spec',
            'object_type',
            'object_url',
            'platform_customizations',
            'product_set_id',
            'status',
            'template_url',
            'template_url_spec',
            'thumbnail_url',
            'title',
            'url_tags',
            'video_id',
        ]
    ),
    AdVideo: collapse_fields_children(
        [
            'id',
            'ad_breaks',
            'backdated_time',
            'backdated_time_granularity',
            'content_tags',
            'created_time',
            'content_category',
            'custom_labels',
            'description',
            'embed_html',
            'embeddable',
            'event',
            'format',
            'from',
            'icon',
            'is_crosspost_video',
            'is_crossposting_eligible',
            'is_instagram_eligible',
            'length',
            'live_status',
            'permalink_url',
            'picture',
            'place',
            'privacy',
            'published',
            'scheduled_publish_time',
            'source',
            'status',
            'title',
            'universal_video_id',
            'updated_time',
        ]
    ),
    CustomAudience: collapse_fields_children(
        [
            'id',
            'account_id',
            'name',
            'approximate_count',
            'data_source',
            'delivery_status',
            'description',
            # `rule` too large objects to download
            # (for more information, see https://operam.atlassian.net/browse/PROD-4298)
            # 'rule',
            'rule_aggregation',
            'subtype',
            'external_event_source',
            'is_value_based',
            'lookalike_audience_ids',
            'lookalike_spec',
            'operation_status',
            'opt_out_link',
            'permission_for_actions',
            'pixel_id',
            'retention_days',
            'time_content_updated',
            'time_created',
            'time_updated',
            # These are Create/Update only fields
            # 'allowed_domains',
            # 'claim_objective',
            # 'content_type',
            # 'dataset_id',
            # 'event_source_group',
            # 'origin_audience_id',
            # 'prefill',
            # 'product_set_id',
            # These fields are not part of the official api docs
            # 'associated_audience_id',
            # 'exclusions',
            # 'inclusions',
            # 'parent_audience_id',
            # 'tags',
        ]
    ),
    Page: collapse_fields_children(
        [
            'about',
            'ad_campaign',
            'affiliation',
            'app_id',
            'app_links',
            'artists_we_like',
            'attire',
            'awards',
            'band_interests',
            'band_members',
            # 'best_page', # requires Page Public Content Access
            'bio',
            'birthday',
            'booking_agent',
            'built',
            'business',
            'can_checkin',
            'can_post',
            'category',
            'category_list',
            'checkins',
            'company_overview',
            'connected_instagram_account',
            'contact_address',
            # 'context',  # silently lost access to this field on April 30, 2019 4AM
            # 'copyright_attribution_insights',  # A page access token is required to request this resource
            # 'copyright_whitelisted_ig_partners',  # A page access token is required to request this resource
            'country_page_likes',
            'cover',
            'culinary_team',
            'current_location',
            'description',
            'description_html',
            'directed_by',
            'display_subtext',
            'displayed_message_response_time',
            'emails',
            'engagement',
            'fan_count',
            'featured_video',
            'features',
            'food_styles',
            'founded',
            'general_info',
            'general_manager',
            'genre',
            'global_brand_page_name',
            'global_brand_root_id',
            'has_whatsapp_number',
            # 'has_added_app', # requires Page Public Content Access
            'hometown',
            'hours',
            'id',
            'impressum',
            'influences',
            'instagram_business_account',
            'is_always_open',
            'is_chain',
            'is_community_page',
            'is_eligible_for_branded_content',
            'is_messenger_platform_bot',
            # requires special permissions (error msg ""The Facebook Page XXXX is not signed up for Instant Articles.")
            # 'instant_articles_review_status',
            'is_messenger_bot_get_started_enabled',
            'is_owned',
            'is_permanently_closed',
            'is_published',
            'is_unclaimed',
            'is_verified',
            'is_webhooks_subscribed',
            'keywords',
            'location',
            'link',
            'members',
            'messenger_ads_default_icebreakers',
            'messenger_ads_default_page_welcome_message',
            'messenger_ads_default_quick_replies',
            'messenger_ads_quick_replies_type',
            'mission',
            'mpg',
            'name',
            'name_with_location_descriptor',
            'network',
            'new_like_count',
            'offer_eligible',
            'overall_star_rating',
            'page_token',
            # 'parent_page', # requires Page Public Content Access
            'parking',
            'payment_options',
            'personal_info',
            'personal_interests',
            'pharma_safety_info',
            'phone',
            'place_type',
            'plot_outline',
            # 'preferred_audience',  # Error msg "Param account_linking_token is required"
            'press_contact',
            'price_range',
            'produced_by',
            'products',
            'promotion_eligible',
            'promotion_ineligible_reason',
            'public_transit',
            'rating_count',
            # 'recipient',  # Error message "(#100) Param account_linking_token is required"
            'record_label',
            'release_date',
            'restaurant_services',
            'restaurant_specialties',
            'talking_about_count',
            'schedule',
            'screenplay_by',
            'season',
            'single_line_address',
            'starring',
            'start_info',
            # 'store_code',  # Error message "(#200) The parent page should be whitelisted for store codes."
            'store_location_descriptor',
            'store_number',
            'studio',
            # 'supports_instant_articles', # requires 'view instant articles"
            'username',
            'unread_message_count',
            'verification_status',
            'unread_notif_count',
            'unseen_message_count',
            'voip_info',
            'website',
            'were_here_count',
            'whatsapp_number',
            'written_by',
        ]
    ),
    PagePost: collapse_fields_children(
        [
            'id',
            'admin_creator',
            'allowed_advertising_objectives',
            'application',
            'backdated_time',
            'call_to_action',
            'attachments',
            # 'can_reply_privately',  # requires READ_PAGE_MAILBOXES or PAGES_MESSAGING permission
            'child_attachments',
            'comments_mirroring_domain',
            'coordinates',
            'created_time',
            'event',
            'expanded_height',
            'expanded_width',
            'feed_targeting',
            'from',
            'full_picture',
            'height',
            'icon',
            'instagram_eligibility',
            'is_app_share',
            'is_expired',
            'is_hidden',
            'is_instagram_eligible',
            'is_popular',
            'is_published',
            'is_eligible_for_promotion',
            'is_spherical',
            'message',
            'message_tags',
            'multi_share_end_card',
            'multi_share_optimized',
            'parent_id',
            'permalink_url',
            'picture',
            'place',
            'privacy',
            'promotable_id',
            'promotion_status',
            'properties',
            'scheduled_publish_time',
            'shares',
            'status_type',
            'story',
            'story_tags',
            'subscribed',
            'target',
            'targeting',
            'timeline_visibility',
            'updated_time',
            'via',
            'video_buying_eligibility',
            'width',
        ]
    ),
    Comment: collapse_fields_children(
        [
            'application',
            'attachment',
            'can_comment',
            # 'can_hide',  # Error message: "(#210) A page access token is required to request this resource."
            'can_like',
            'can_remove',
            # 'can_reply_privately',
            'comment_count',
            'created_time',
            'from',
            'id',
            'is_hidden',
            'is_private',
            'like_count',
            'live_broadcast_timestamp',
            'message',
            'message_tags',
            'object',
            'parent',
            'parent_id',
            'permalink_url',
            # Error message: "(#200) The page does not have READ_PAGE_MAILBOXES or PAGES_MESSAGING permission."
            # 'private_reply_conversation',
            'user_likes',
            # Reactions
            'reactions.type(LIKE).summary(true).limit(0).as(reaction_like)',
            'reactions.type(LOVE).summary(true).limit(0).as(reaction_love)',
            'reactions.type(WOW).summary(true).limit(0).as(reaction_wow)',
            'reactions.type(HAHA).summary(true).limit(0).as(reaction_haha)',
            'reactions.type(SAD).summary(true).limit(0).as(reaction_sad)',
            'reactions.type(ANGRY).summary(true).limit(0).as(reaction_angry)',
            'reactions.type(THANKFUL).summary(true).limit(0).as(reaction_thankful)',
        ]
    ),
}


def get_default_fields(model_klass: Type['Model']) -> List[str]:
    """
    Obtain default fields for a given entity type. Note that the entity
    class must come from the Facebook SDK
    """
    assert issubclass(model_klass, abstractcrudobject.AbstractCrudObject)

    if model_klass in _default_fields_map:
        return _default_fields_map[model_klass]

    return [
        getattr(model_klass.Field, field_name)
        for field_name in dir(model_klass.Field)
        if not field_name.startswith('__')
    ]


# defaults for most of these are some 20-25
# at that level paging through tens of thousands of results is super painful (and long, obviously)
# Hence bumping the page size for each, but the larger the size, the more change
# is there for too much data on the page killing the request.
# So, if you start seeing chronic failures in fetches of particular type of object across
# all AdAccounts, push the number lower.
# If you see problems with particular AdAccount (that likes to use lots of DMA, Zipcodes for targeting, for example)
# that would be the time for tuning page sizes per AdAccount or speculatively adapt page size
# in evidence of errors in prior attempts.
# There is also a nice side-effect to shifting this to FB - each request outstanding
# runs longer and allows greater concurrency locally.
_default_page_size = {
    Campaign: 400,
    AdSet: 200,  # this is super heavy object mostly because of Targeting spec. Keep it smallish
    Ad: 400,
    Comment: 100,
    CustomAudience: 50,
}

DEFAULT_PAGE_ACCESS_TOKEN_LIMIT = 250


def get_default_page_size(model_klass: Type['Model']) -> int:
    """
    Default paging size on FB API is too small for large collections
    It's usually some 25 items. We page through a lot of stuff, hence this fn.
    """
    assert issubclass(model_klass, abstractcrudobject.AbstractCrudObject)

    return _default_page_size.get(model_klass, 100)


_default_additional_params = {Comment: {'filter': 'stream'}}


def get_additional_params(model_klass: Type['Model']) -> Dict[str, Any]:
    """
    By default, we dont need additional params to FB API requests. But in some instances (i.e. fetching Comments),
    adding parameters makes fetching data simpler
    """
    assert issubclass(model_klass, abstractcrudobject.AbstractCrudObject)

    return _default_additional_params.get(model_klass, {})


# By default ARCHIVED is filtered out
# Here we repeat all possible status values we get by default
# and include ARCHIVED into the set.
# It's unfortunate that facebook does not yet allow to filter by configured status.
_default_fetch_statuses = {
    # Note that at Campaign level asking for "Pending*" or **approve*
    # statuses is not allowed - results in argument validation error
    # this is largely related to fact that Campaigns don't need to be approved,
    # and billing is done mostly at AdSet level
    # So, in a way it makes sense.
    Campaign: [
        'ACTIVE',
        # 'ADSET_PAUSED',
        'ARCHIVED',
        # 'CAMPAIGN_PAUSED',
        # 'DELETED',
        # 'DISAPPROVED',
        'PAUSED',
        # 'PENDING_BILLING_INFO',
        # 'PENDING_REVIEW',
        # 'PREAPPROVED',
        'IN_PROCESS',
    ],
    AdSet: [
        'ACTIVE',
        # 'ADSET_PAUSED',
        'ARCHIVED',
        'CAMPAIGN_PAUSED',
        # 'DELETED',
        'DISAPPROVED',
        'PAUSED',
        'PENDING_BILLING_INFO',
        'PENDING_REVIEW',
        'PREAPPROVED',
        'IN_PROCESS',
    ],
    Ad: [
        'ACTIVE',
        'ADSET_PAUSED',
        'ARCHIVED',
        'CAMPAIGN_PAUSED',
        # 'DELETED',
        'DISAPPROVED',
        'PAUSED',
        'PENDING_BILLING_INFO',
        'PENDING_REVIEW',
        'PREAPPROVED',
        'IN_PROCESS',
    ],
}


def get_default_status(model_klass: Type['Model']) -> List[str]:
    """
    Each Entity Level has its own set of possible valid status values
    acceptable as filtering parameters for "get all per parent AA" calls.

    What we are trying to solve here is remove the default filter for Archived
    from all calls by repeating all possible fetch-able effective status values
    per that FB Entity Level, including Archived
    """
    assert issubclass(model_klass, abstractcrudobject.AbstractCrudObject)
    return _default_fetch_statuses.get(model_klass)
