from typing import Generator, List

from facebookads.api import FacebookAdsApi, FacebookSession
from facebookads.adobjects import abstractcrudobject
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.campaign import Campaign
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.ad import Ad
from oozer.common.enum import to_fb_model


class FacebookApiContext:
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

    THROTTLING_CODES = [
        (4, None),  # Application request limit reached
        (17, None),  # User request limit reached
        (613, 1487742),  # AdAccount request limit reached
    ]
    """
    List of known codes (subcodes) where FB starts throttling us
    """

    TOO_MUCH_DATA_CODES = [
        (100, 1487534)  # Too big a report
    ]
    """
    List of known codes (subcodes) where FB complains about us asking for too
    much data 
    """

    _exception = None

    def __init__(self, exception):
        """
        Store the exception we want to test

        :param FacebookRequestError exception: The Facebook Exception
        """
        self._exception = exception

    def _is_exception_in_list(self, values):
        """
        Check an exception against a given list of possible codes/subcodes


        :param list values: List of individual codes or tuples of (code, subcode)
        :return bool: The exception conforms to our excepted list
        """
        code = self._exception.api_error_code()
        subcode = self._exception.api_error_subcode()

        return (code, subcode) in values

    def is_throttling_exception(self):
        """
        Checks whether given Facebook Exception is of throttling type

        :param FacebookRequestError exception: The Facebook Exception
        :return bool: If True, the exception is of type throttling
        """
        return self._is_exception_in_list(self.THROTTLING_CODES)

    def is_too_large_data_exception(self):
        """
        Checks whether given Facebook Exception is of a type that says "you are
        asking me to do / calculate too much"

        :param FacebookRequestError exception: The Facebook Exception
        :return bool: If True, the exception is of type "too much data"
        """
        return self._is_exception_in_list(self.TOO_MUCH_DATA_CODES)


_default_fields_map = {
    # AdAccount: ['id'],
    Campaign: [
        'account_id',
        # 'adlabels',
        # 'boosted_object_id',
        # 'brand_lift_studies',
        # 'budget_rebalance_flag',
        'buying_type',
        # 'can_create_brand_lift_study',
        # 'can_use_spend_cap',
        # 'configured_status',
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
    ],
    AdSet: [
        'account_id',
        # 'adlabels',
        # 'adset_schedule',
        # 'attribution_spec',
        # 'bid_amount',
        'bid_info',
        # 'billing_event',
        # 'budget_remaining',
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
        # 'is_autobid',
        # 'is_average_price_pacing',
        'lifetime_budget',
        # 'lifetime_imps',
        'name',
        # 'optimization_goal',
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
        # 'targeting',  # Yes we need it, but how? https://drive.google.com/drive/folders/1R01e7WiilzKDPYTKpVnUXQQyIUldFmCK
        # 'time_based_ad_rotation_id_blocks',
        # 'time_based_ad_rotation_intervals',
        'updated_time',
        # 'use_new_app_click'
    ],
    Ad: [
        'account_id',
        # 'ad_review_feedback', <----- !!!!!!!!!
        # 'adlabels', <----- !!!!!!!!!
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
        # 'creative{id,effective_instagram_story_id,effective_object_story_id}'
        'creative', # 'id' field is communicated by default
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
    ]
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
