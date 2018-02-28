from typing import Generator

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



def get_default_fields(Model):
    # type: (Model) -> Generator[str]
    """
    Obtain default fields for a given entity type. Note that the entity
    class must come from the Facebook SDK

    :param Model:
    :rtype: Generator[str] of fields
    """
    assert issubclass(Model, abstractcrudobject.AbstractCrudObject)

    for field_name in dir(Model.Field):
        if field_name.startswith('__'):
            continue

        yield getattr(Model.Field, field_name)
