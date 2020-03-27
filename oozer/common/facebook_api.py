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
            AdAccount.Field.id,
            AdAccount.Field.account_id,
            AdAccount.Field.name,
            AdAccount.Field.account_status,
            AdAccount.Field.amount_spent,
            AdAccount.Field.attribution_spec,
            AdAccount.Field.can_create_brand_lift_study,
            AdAccount.Field.capabilities,
            AdAccount.Field.currency,
            AdAccount.Field.end_advertiser,
            AdAccount.Field.end_advertiser_name,
            AdAccount.Field.owner,
            AdAccount.Field.rf_spec,
            AdAccount.Field.spend_cap,
            AdAccount.Field.timezone_id,
            AdAccount.Field.timezone_name,
            AdAccount.Field.timezone_offset_hours_utc,
        ]
    ),
    Campaign: collapse_fields_children(
        [
            Campaign.Field.account_id,
            Campaign.Field.adlabels,
            Campaign.Field.buying_type,
            Campaign.Field.can_create_brand_lift_study,
            Campaign.Field.can_use_spend_cap,
            Campaign.Field.created_time,
            Campaign.Field.effective_status,
            Campaign.Field.id,
            Campaign.Field.name,
            Campaign.Field.objective,
            Campaign.Field.spend_cap,
            Campaign.Field.start_time,
            Campaign.Field.status,
            Campaign.Field.stop_time,
            Campaign.Field.updated_time,
        ]
    ),
    AdSet: collapse_fields_children(
        [
            AdSet.Field.account_id,
            AdSet.Field.bid_amount,
            AdSet.Field.bid_info,
            AdSet.Field.billing_event,
            AdSet.Field.budget_remaining,
            AdSet.Field.campaign_id,
            AdSet.Field.created_time,
            AdSet.Field.daily_budget,
            AdSet.Field.effective_status,
            AdSet.Field.end_time,
            AdSet.Field.id,
            AdSet.Field.bid_strategy,
            AdSet.Field.lifetime_budget,
            AdSet.Field.name,
            AdSet.Field.optimization_goal,
            AdSet.Field.start_time,
            AdSet.Field.status,
            AdSet.Field.targeting,
            AdSet.Field.updated_time,
        ]
    ),
    Ad: collapse_fields_children(
        [
            Ad.Field.account_id,
            Ad.Field.adset_id,
            Ad.Field.campaign_id,
            Ad.Field.created_time,
            Ad.Field.effective_status,
            Ad.Field.id,
            Ad.Field.last_updated_by_app_id,
            Ad.Field.name,
            Ad.Field.source_ad_id,
            Ad.Field.status,
            Ad.Field.tracking_specs,
            Ad.Field.updated_time,
            (
                Ad.Field.creative,
                [
                    AdCreative.Field.effective_instagram_story_id,
                    AdCreative.Field.effective_object_story_id,
                    AdCreative.Field.id,
                    AdCreative.Field.name,
                ],
            ),
        ]
    ),
    AdCreative: collapse_fields_children(
        [
            AdCreative.Field.id,
            AdCreative.Field.account_id,
            AdCreative.Field.actor_id,
            AdCreative.Field.adlabels,
            AdCreative.Field.applink_treatment,
            AdCreative.Field.asset_feed_spec,
            AdCreative.Field.body,
            AdCreative.Field.branded_content_sponsor_page_id,
            AdCreative.Field.call_to_action_type,
            AdCreative.Field.effective_instagram_story_id,
            AdCreative.Field.effective_object_story_id,
            AdCreative.Field.image_crops,
            AdCreative.Field.image_hash,
            AdCreative.Field.image_url,
            AdCreative.Field.instagram_actor_id,
            AdCreative.Field.instagram_permalink_url,
            AdCreative.Field.instagram_story_id,
            AdCreative.Field.link_og_id,
            AdCreative.Field.link_url,
            AdCreative.Field.name,
            AdCreative.Field.object_id,
            AdCreative.Field.object_story_id,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.object_type,
            AdCreative.Field.object_url,
            AdCreative.Field.platform_customizations,
            AdCreative.Field.product_set_id,
            AdCreative.Field.status,
            AdCreative.Field.template_url,
            AdCreative.Field.template_url_spec,
            AdCreative.Field.thumbnail_url,
            AdCreative.Field.title,
            AdCreative.Field.url_tags,
            AdCreative.Field.video_id,
        ]
    ),
    AdVideo: collapse_fields_children(
        [
            AdVideo.Field.id,
            AdVideo.Field.ad_breaks,
            AdVideo.Field.backdated_time,
            AdVideo.Field.backdated_time_granularity,
            AdVideo.Field.content_tags,
            AdVideo.Field.created_time,
            AdVideo.Field.content_category,
            AdVideo.Field.custom_labels,
            AdVideo.Field.description,
            AdVideo.Field.embed_html,
            AdVideo.Field.embeddable,
            AdVideo.Field.event,
            AdVideo.Field.format,
            AdVideo.Field.field_from,
            AdVideo.Field.icon,
            AdVideo.Field.is_crosspost_video,
            AdVideo.Field.is_crossposting_eligible,
            AdVideo.Field.is_instagram_eligible,
            AdVideo.Field.length,
            AdVideo.Field.live_status,
            AdVideo.Field.permalink_url,
            AdVideo.Field.picture,
            AdVideo.Field.place,
            AdVideo.Field.privacy,
            AdVideo.Field.published,
            AdVideo.Field.scheduled_publish_time,
            AdVideo.Field.source,
            AdVideo.Field.status,
            AdVideo.Field.title,
            AdVideo.Field.universal_video_id,
            AdVideo.Field.updated_time,
        ]
    ),
    CustomAudience: collapse_fields_children(
        [
            CustomAudience.Field.id,
            CustomAudience.Field.account_id,
            CustomAudience.Field.name,
            CustomAudience.Field.approximate_count,
            CustomAudience.Field.data_source,
            CustomAudience.Field.delivery_status,
            CustomAudience.Field.description,
            CustomAudience.Field.rule_aggregation,
            CustomAudience.Field.subtype,
            CustomAudience.Field.external_event_source,
            CustomAudience.Field.is_value_based,
            CustomAudience.Field.lookalike_audience_ids,
            CustomAudience.Field.lookalike_spec,
            CustomAudience.Field.operation_status,
            CustomAudience.Field.opt_out_link,
            CustomAudience.Field.permission_for_actions,
            CustomAudience.Field.pixel_id,
            CustomAudience.Field.retention_days,
            CustomAudience.Field.time_content_updated,
            CustomAudience.Field.time_created,
            CustomAudience.Field.time_updated,
            # `rule` too large objects to download
            # (for more information, see https://operam.atlassian.net/browse/PROD-4298)
            # 'rule',
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
            Page.Field.about,
            Page.Field.ad_campaign,
            Page.Field.affiliation,
            Page.Field.app_id,
            # Page.Field.app_links, # quietly removed in v6.0
            Page.Field.artists_we_like,
            Page.Field.attire,
            Page.Field.awards,
            Page.Field.band_interests,
            Page.Field.band_members,
            # Page.Field.best_page', # requires Page Public Content Access
            Page.Field.bio,
            Page.Field.birthday,
            Page.Field.booking_agent,
            Page.Field.built,
            Page.Field.business,
            Page.Field.can_checkin,
            Page.Field.can_post,
            Page.Field.category,
            Page.Field.category_list,
            Page.Field.checkins,
            Page.Field.company_overview,
            Page.Field.connected_instagram_account,
            Page.Field.contact_address,
            # Page.Field.context',  # silently lost access to this field on April 30, 2019 4AM
            # Page.Field.copyright_attribution_insights',  # A page access token is required to request this resource
            # Page.Field.copyright_whitelisted_ig_partners',  # A page access token is required to request this resource
            Page.Field.country_page_likes,
            Page.Field.cover,
            Page.Field.culinary_team,
            Page.Field.current_location,
            Page.Field.description,
            Page.Field.description_html,
            Page.Field.directed_by,
            Page.Field.display_subtext,
            Page.Field.displayed_message_response_time,
            Page.Field.emails,
            Page.Field.engagement,
            Page.Field.fan_count,
            Page.Field.featured_video,
            Page.Field.features,
            Page.Field.food_styles,
            Page.Field.founded,
            Page.Field.general_info,
            Page.Field.general_manager,
            Page.Field.genre,
            Page.Field.global_brand_page_name,
            Page.Field.global_brand_root_id,
            # Page.Field.has_added_app', # requires Page Public Content Access
            Page.Field.has_whatsapp_number,
            Page.Field.hometown,
            Page.Field.hours,
            Page.Field.id,
            Page.Field.impressum,
            Page.Field.influences,
            Page.Field.instagram_business_account,
            # Page.Field.instant_articles_review_status',
            Page.Field.is_always_open,
            Page.Field.is_chain,
            Page.Field.is_community_page,
            Page.Field.is_eligible_for_branded_content,
            Page.Field.is_messenger_bot_get_started_enabled,
            Page.Field.is_messenger_platform_bot,
            Page.Field.is_owned,
            Page.Field.is_permanently_closed,
            Page.Field.is_published,
            Page.Field.is_unclaimed,
            Page.Field.is_verified,
            Page.Field.is_webhooks_subscribed,
            Page.Field.keywords,
            Page.Field.link,
            Page.Field.location,
            Page.Field.members,
            Page.Field.messenger_ads_default_icebreakers,
            Page.Field.messenger_ads_default_page_welcome_message,
            Page.Field.messenger_ads_default_quick_replies,
            Page.Field.messenger_ads_quick_replies_type,
            Page.Field.mission,
            Page.Field.mpg,
            Page.Field.name,
            Page.Field.name_with_location_descriptor,
            Page.Field.network,
            Page.Field.new_like_count,
            Page.Field.offer_eligible,
            Page.Field.overall_star_rating,
            Page.Field.page_token,
            # Page.Field.parent_page', # requires Page Public Content Access
            Page.Field.parking,
            Page.Field.payment_options,
            Page.Field.personal_info,
            Page.Field.personal_interests,
            Page.Field.pharma_safety_info,
            Page.Field.phone,
            Page.Field.place_type,
            Page.Field.plot_outline,
            # Page.Field.preferred_audience',  # Error msg "Param account_linking_token is required"
            Page.Field.press_contact,
            Page.Field.price_range,
            Page.Field.produced_by,
            Page.Field.products,
            Page.Field.promotion_eligible,
            Page.Field.promotion_ineligible_reason,
            Page.Field.public_transit,
            Page.Field.rating_count,
            # Page.Field.recipient',  # Error message "(#100) Param account_linking_token is required"
            Page.Field.record_label,
            Page.Field.release_date,
            Page.Field.restaurant_services,
            Page.Field.restaurant_specialties,
            Page.Field.schedule,
            Page.Field.screenplay_by,
            Page.Field.season,
            Page.Field.single_line_address,
            Page.Field.starring,
            Page.Field.start_info,
            # Page.Field.store_code',  # Error message "(#200) The parent page should be whitelisted for store codes."
            Page.Field.store_location_descriptor,
            Page.Field.store_number,
            Page.Field.studio,
            # Page.Field.supports_instant_articles', # requires 'view instant articles"
            Page.Field.talking_about_count,
            Page.Field.unread_message_count,
            Page.Field.unread_notif_count,
            Page.Field.unseen_message_count,
            Page.Field.username,
            Page.Field.verification_status,
            Page.Field.voip_info,
            Page.Field.website,
            Page.Field.were_here_count,
            Page.Field.whatsapp_number,
            Page.Field.written_by,
        ]
    ),
    PagePost: collapse_fields_children(
        [
            # 'can_reply_privately',  # requires READ_PAGE_MAILBOXES or PAGES_MESSAGING permission
            PagePost.Field.admin_creator,
            PagePost.Field.allowed_advertising_objectives,
            PagePost.Field.application,
            'attachments',  # PagePost.Field.attachments, <- :( not official attribute
            PagePost.Field.backdated_time,
            PagePost.Field.call_to_action,
            PagePost.Field.child_attachments,
            PagePost.Field.comments_mirroring_domain,
            PagePost.Field.coordinates,
            PagePost.Field.created_time,
            PagePost.Field.event,
            PagePost.Field.expanded_height,
            PagePost.Field.expanded_width,
            PagePost.Field.feed_targeting,
            PagePost.Field.field_from,
            PagePost.Field.full_picture,
            PagePost.Field.height,
            PagePost.Field.icon,
            PagePost.Field.id,
            PagePost.Field.instagram_eligibility,
            PagePost.Field.is_app_share,
            PagePost.Field.is_eligible_for_promotion,
            PagePost.Field.is_expired,
            PagePost.Field.is_hidden,
            PagePost.Field.is_instagram_eligible,
            PagePost.Field.is_popular,
            PagePost.Field.is_published,
            PagePost.Field.is_spherical,
            PagePost.Field.message,
            PagePost.Field.message_tags,
            PagePost.Field.multi_share_end_card,
            PagePost.Field.multi_share_optimized,
            PagePost.Field.parent_id,
            PagePost.Field.permalink_url,
            PagePost.Field.picture,
            PagePost.Field.place,
            PagePost.Field.privacy,
            PagePost.Field.promotable_id,
            PagePost.Field.promotion_status,
            PagePost.Field.properties,
            PagePost.Field.scheduled_publish_time,
            PagePost.Field.shares,
            PagePost.Field.status_type,
            PagePost.Field.story,
            PagePost.Field.story_tags,
            PagePost.Field.subscribed,
            PagePost.Field.target,
            PagePost.Field.targeting,
            PagePost.Field.timeline_visibility,
            PagePost.Field.updated_time,
            PagePost.Field.via,
            PagePost.Field.video_buying_eligibility,
            PagePost.Field.width,
        ]
    ),
    Comment: collapse_fields_children(
        [
            Comment.Field.application,
            Comment.Field.attachment,
            Comment.Field.can_comment,
            Comment.Field.can_like,
            Comment.Field.can_remove,
            Comment.Field.comment_count,
            Comment.Field.created_time,
            Comment.Field.field_from,
            Comment.Field.id,
            Comment.Field.is_hidden,
            Comment.Field.is_private,
            Comment.Field.like_count,
            Comment.Field.live_broadcast_timestamp,
            Comment.Field.message,
            Comment.Field.message_tags,
            Comment.Field.object,
            Comment.Field.parent,
            'parent_id',  # Comment.Field.parent_id, # Not official?
            Comment.Field.permalink_url,
            Comment.Field.user_likes,
            # 'can_reply_privately',
            # 'can_hide',  # Error message: "(#210) A page access token is required to request this resource."
            # 'private_reply_conversation',
            # Error message: "(#200) The page does not have READ_PAGE_MAILBOXES or PAGES_MESSAGING permission."
            # Reactions edge traversal
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
