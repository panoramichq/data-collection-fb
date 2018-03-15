# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.enums.failure_bucket import FailureBucket
from common.tokens import PlatformTokenManager
from tests.base.random import get_string_id


class TestingTokenManager(TestCase):

    def setUp(self):
        super().setUp()
        self.sweep_id = get_string_id()  # <-- generates random ID for each test run
        self.asset_scope = get_string_id()
        self.token_manager = PlatformTokenManager(self.asset_scope, self.sweep_id)

    def test_empty_state(self):
        token_manager = self.token_manager

        assert token_manager.get_token_count() == 0

    def test_adding_token(self):
        token_manager = self.token_manager

        token = '123'
        token_manager.add(token)
        assert token_manager.get_token_count() == 1
        assert token_manager.get_best_token() == token

    def test_token_priority(self):
        token_manager = self.token_manager

        first_token = 'first'
        second_token = 'second'

        token_manager.add(first_token, second_token)

        assert token_manager.get_token_count() == 2

        # Used the first token
        token_manager.report_usage(first_token)

        assert token_manager.get_best_token() == second_token

    def test_token_priority_with_failure_buckets(self):
        token_manager = self.token_manager

        first_token = 'first'
        second_token = 'second'
        third_token = 'third'

        token_manager.add(first_token, second_token, third_token)

        assert token_manager.get_token_count() == 3

        # Used the first token
        token_manager.report_usage_per_failure_bucket(first_token, FailureBucket.Throttling) # most penalized
        token_manager.report_usage_per_failure_bucket(third_token, FailureBucket.TooLarge)  # somewhat heavily penalized
        token_manager.report_usage_per_failure_bucket(second_token, 'blah')  # gets default 1 use

        # best token is one with least penalty / use
        assert token_manager.get_best_token() == second_token
        token_manager.remove(second_token)

        # least worst of remaining ones
        assert token_manager.get_best_token() == third_token
        token_manager.remove(third_token)

        # well... it's only one left
        assert token_manager.get_best_token() == first_token
        token_manager.remove(first_token)

        # allow None to be returned
        assert token_manager.get_best_token() is None
