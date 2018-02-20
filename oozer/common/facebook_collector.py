from facebookads.api import FacebookAdsApi, FacebookSession
from facebookads.adobjects import (
    abstractcrudobject,
    adaccount
)


class FacebookCollector:
    """
    A simple wrapper for Facebook SDK, using local API sessions as not to
    pollute the the global default API session with initialization
    """

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

    def _get_ad_account(self, account_id):
        """
        Construct the Ad Account object using the id without the `act_` bit

        :param account_id:
        :return:
        """
        return adaccount.AdAccount(fbid=f'act_{account_id}', api=self.api)

    def _get_default_fileds(self, Model):
        """
        Obtain default fields for a given entity type. Note that the entity
        class must come from the Facebook SDK

        :param Model:
        :return:
        """
        assert issubclass(Model, abstractcrudobject.AbstractCrudObject)

        return filter(
            lambda val: not val.startswith('__'),
            dir(Model.Field)
        )
