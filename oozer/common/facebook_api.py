from typing import List

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi, FacebookSession
from facebook_business.adobjects import abstractcrudobject
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.exceptions import FacebookRequestError

from common.enums.failure_bucket import FailureBucket
from oozer.common.enum import to_fb_model, ExternalPlatformJobStatus
from oozer.common.facebook_fields import collapse_fields_children


class PlatformApiContext:
    """
    A simple wrapper for Facebook SDK, using local API sessions as not to
    pollute the the global default API session with initialization
    """

    token = None  # type: str
    api = None  # type: FacebookAdsApi

    def __init__(self, token):
        """
        :param token:
        """
        self.token = token

    def __enter__(self):
        """

        """
        self.api = FacebookAdsApi(FacebookSession(access_token=self.token))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        We do not need to do anything specific yet
        """
        pass

    def to_fb_model(self, entity_id, entity_type):
        """
        Like stand-alone to_fb_model but removes the need to pass in API
        instance manually

        :param string entity_id: The entity ID
        :param entity_type:
        :return:
        """
        return to_fb_model(entity_id, entity_type, self.api)


class FacebookApiErrorInspector:
    """
    A vehicle to store the information on distinct *types* of errors that FB
    cna throw at us and we're interested in them
    """

    GENERIC_ERROR_CODE = 1

    THROTTLING_CODES = {
        (4, None),  # Application request limit reached
        (17, None),  # User request limit reached
        (17, 2446079),  # User request limit reached
        (613, 1487742),  # AdAccount request limit reached
    }
    """
    List of known codes (subcodes) where FB starts throttling us
    """

    TOO_MUCH_DATA_CODES = {
        (100, 1487534)  # Too big a report
    }
    """
    List of known codes (subcodes) where FB complains about us asking for too
    much data
    """

    TOO_MUCH_DATA_MESSAGES = {
        "Please reduce the amount of data you're asking for, then retry your request",
    }

    _exception = None

    def __init__(self, exception):
        """
        Store the exception we want to test

        :param FacebookError exception: The Facebook Exception
        """
        self._exception = exception

    @property
    def _is_generic_error(self) -> bool:
        """Check if exception is a generic error."""
        return self.GENERIC_ERROR_CODE == self._exception.api_error_code()

    def _is_exception_code_in_set(self, values) -> bool:
        """
        Check the exception code and subcode a given set.

        :param set values: List of individual codes or tuples of (code, subcode)
        :return bool: The exception conforms to our excepted list
        """
        if not isinstance(self._exception, FacebookRequestError):
            return False

        code = self._exception.api_error_code()
        subcode = self._exception.api_error_subcode()

        return (code, subcode) in values

    def _is_exception_message_in_set(self, values) -> bool:
        """
        Check the exception error message against a given set.
        :param values:
        :return:
        """
        if not isinstance(self._exception, FacebookRequestError):
            return False

        return self._exception.api_error_message() in values

    def is_throttling_exception(self) -> bool:
        """
        Checks whether given Facebook Exception is of throttling type

        :param FacebookRequestError exception: The Facebook Exception
        :return bool: If True, the exception is of type throttling
        """
        return self._is_exception_code_in_set(self.THROTTLING_CODES)

    def is_too_large_data_exception(self) -> bool:
        """
        Checks whether given Facebook Exception is of a type that says "you are
        asking me to do / calculate too much"

        :param FacebookRequestError exception: The Facebook Exception
        :return bool: If True, the exception is of type "too much data"
        """
        return self._is_exception_code_in_set(self.TOO_MUCH_DATA_CODES) or (
            self._is_generic_error and self._is_exception_message_in_set(self.TOO_MUCH_DATA_MESSAGES)
        )

    def get_status_and_bucket(self) -> (int, int):
        """Extract status and bucket from inspected exception."""
        # Is this a throttling error?
        if self.is_throttling_exception():
            failure_status = ExternalPlatformJobStatus.ThrottlingError
            failure_bucket = FailureBucket.Throttling

        # Did we ask for too much data?
        elif self.is_too_large_data_exception():
            failure_status = ExternalPlatformJobStatus.TooMuchData
            failure_bucket = FailureBucket.TooLarge

        # It's something else which we don't understand
        else:
            failure_status = ExternalPlatformJobStatus.GenericPlatformError
            failure_bucket = FailureBucket.Other

        return failure_status, failure_bucket


_default_fields_map = {
    AdAccount: collapse_fields_children([
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
    ]),
    Campaign: collapse_fields_children([
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
        'updated_time'
    ]),
    AdSet: collapse_fields_children([
        'account_id',
        # 'adlabels',
        # 'adset_schedule',
        # 'attribution_spec',
        'bid_amount',
        'bid_info',
        'billing_event',
        'budget_remaining',  #TODO: Temp enable. Migrate Console away from use.
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
        'targeting',  # Yes we need it, but beware! https://drive.google.com/drive/folders/1R01e7WiilzKDPYTKpVnUXQQyIUldFmCK
        # 'time_based_ad_rotation_id_blocks',
        # 'time_based_ad_rotation_intervals',
        'updated_time',
        # 'use_new_app_click'
    ]),
    Ad: collapse_fields_children([
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
        (
            'creative',
            [
                'id',
                'effective_instagram_story_id',
                'effective_object_story_id',
                'name'
            ]
        ),
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
        'tracking_specs', # <----- !!!!!!!!!
        'updated_time'
    ]),
    AdCreative: collapse_fields_children([
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
    ]),
    AdVideo: collapse_fields_children([
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
    ]),
    CustomAudience: collapse_fields_children([
        'id',
        'account_id',
        'name',
        'approximate_count',
        'data_source',
        'delivery_status',
        'description',
        'rule',
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

        # These fields are not part of the oficial api docs
        # 'associated_audience_id',
        # 'exclusions',
        # 'inclusions',
        # 'parent_audience_id',
        # 'tags',
    ])
}


def get_default_fields(Model):
    # type: (Model) -> List[str]
    """
    Obtain default fields for a given entity type. Note that the entity
    class must come from the Facebook SDK

    :param Model:
    :rtype: List[str] of fields
    """
    assert issubclass(Model, abstractcrudobject.AbstractCrudObject)

    if Model in _default_fields_map:
        return _default_fields_map[Model]

    return [
        getattr(Model.Field, field_name)
        for field_name in dir(Model.Field)
        if not field_name.startswith('__')
    ]


# defaults for most of these are some 20-25
# at that level paging throigh tens of thousands of results is super painful (and long, obviously)
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
    Ad: 400
}

def get_default_page_size(Model):
    # type: (Model) -> List[str]
    """
    Default paging size on FB API is too small for large collections
    It's usually some 25 items. We page through a lot of stuff, hence this fn.

    :param Model:
    :rtype: List[str] of fields
    """
    assert issubclass(Model, abstractcrudobject.AbstractCrudObject)

    return _default_page_size.get(Model)


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
    ]
}


def get_default_status(Model):
    # type: (Model) -> List[str]
    """
    Each Entity Level has its own set of possible valid status values
    acceptable as filtering parameters for "get all per parent AA" calls.

    What we are trying to solve here is remove the default filter for Archived
    from all calls by repeating all possible fetch-able effective status values
    per that FB Entity Level, including Archived

    :param Model:
    :rtype: List[str] of fields
    """
    assert issubclass(Model, abstractcrudobject.AbstractCrudObject)
    return _default_fetch_statuses.get(Model)
