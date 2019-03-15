# flake8: noqa: F401

# Patches must be imported and ran first
from common.patch import patch_event_loop
patch_event_loop()
from common.facebook.patch import patch_facebook_sdk
patch_facebook_sdk()

import pkgutil

from common.configure_logging import configure_logging
from config.facebook import AD_ACCOUNT, AD_ACCOUNT_TIME_ZONE, TOKEN
from importlib import import_module
from unittest import TestCase as _TestCase, skip, mock

configure_logging()


class TestCase(_TestCase):
    """
    Base TestCase class for all tests in our system

    While at this time it does not implement / scaffold anything new
    on top of typical base test class, it may in the future.

    The most important thing, actually is
    **forcing all tests to go through this module.**
    This is, effectively, the primary entry point for all tests in the system,
    and, thus, is the best (until we find a new one) place to patch
    event loop, set up connections etc - all centrally.
    """

    def setUp(self):
        "Hook method for setting up the test fixture before exercising it."
        pass

    def tearDown(self):
        "Hook method for deconstructing the test fixture after testing it."
        pass

    @classmethod
    def setUpClass(cls):
        "Hook method for setting up class fixture before running tests in the class."

    @classmethod
    def tearDownClass(cls):
        "Hook method for deconstructing the class fixture after running all tests in the class."


class IntegrationTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.ad_account_id = AD_ACCOUNT
        self.ad_account_time_zone = AD_ACCOUNT_TIME_ZONE
        self.token = TOKEN


def integration(module='facebook'):
    def check_module(fn):
        allowed_modules = map(lambda x: x.name, pkgutil.iter_modules(['config']))
        assert module in allowed_modules

        MODULE_TOKEN = import_module(f'config.{module}').TOKEN
        if MODULE_TOKEN and MODULE_TOKEN != 'bogus token':
            # it's overridden only in dev. In all other cases should be that bogus value
            return fn
        else:
            return skip(fn)

    return check_module
