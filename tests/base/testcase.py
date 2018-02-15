from common.patch import patch_event_loop
patch_event_loop()

from unittest import TestCase as _TestCase


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