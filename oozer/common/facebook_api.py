from facebookads.api import FacebookAdsApi, FacebookSession
from facebookads.adobjects import abstractcrudobject
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


def get_default_fields(Model):
    """
    Obtain default fields for a given entity type. Note that the entity
    class must come from the Facebook SDK

    :param Model:
    :return generator: List of fields
    """
    assert issubclass(Model, abstractcrudobject.AbstractCrudObject)

    for field_name in dir(Model.Field):
        if field_name.startswith('__'):
            continue

        yield getattr(Model.Field, field_name)
