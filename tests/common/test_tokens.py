# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.store.scope import AssetScope
from common.tokens import PlatformTokenManager
from oozer.common.job_scope import JobScope
from tests.base.random import gen_string_id


class TestingTokenManager(TestCase):
    def setUp(self):
        super().setUp()
        self.sweep_id = gen_string_id()  # <-- generates random ID for each test run
        self.asset_scope = gen_string_id()

    def test_empty_state(self):
        token_manager = PlatformTokenManager(self.asset_scope, self.sweep_id)

        assert token_manager.get_token_count() == 0

    def test_adding_token(self):
        token_manager = PlatformTokenManager(self.asset_scope, self.sweep_id)

        token = '123'
        token_manager.add(token)
        assert token_manager.get_token_count() == 1
        assert token_manager.get_best_token() == token

    def test_token_priority(self):
        token_manager = PlatformTokenManager(self.asset_scope, self.sweep_id)

        first_token = 'first'
        second_token = 'second'

        token_manager.add(first_token, second_token)

        assert token_manager.get_token_count() == 2

        # Used the first token
        token_manager.report_usage(first_token)

        assert token_manager.get_best_token() == second_token

    def test_token_priority_with_failure_buckets(self):
        token_manager = PlatformTokenManager(self.asset_scope, self.sweep_id)

        first_token = 'first'
        second_token = 'second'
        third_token = 'third'

        token_manager.add(first_token, second_token, third_token)

        assert token_manager.get_token_count() == 3

        # Used the first token
        token_manager.report_usage_per_failure_bucket(first_token, FailureBucket.Throttling)  # most penalized
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

    def test_from_job_scope(self):

        key_gen = '{asset_scope}-{sweep_id}-sorted-token-queue'.format

        sweep_id = gen_string_id()
        entity_id = gen_string_id()
        scope_id = gen_string_id()

        # Scope-centered jobs must result in scope-centered key for token storage
        job_scope = JobScope(sweep_id=sweep_id, entity_type=Entity.Scope, entity_id=scope_id)
        token_manager = PlatformTokenManager.from_job_scope(job_scope)
        assert token_manager.queue_key == key_gen(asset_scope=scope_id, sweep_id=sweep_id)

        # non-Scope-centered jobs must result in 'fb'-centered key for token storage
        job_scope = JobScope(sweep_id=sweep_id)
        token_manager = PlatformTokenManager.from_job_scope(job_scope)
        assert token_manager.queue_key == key_gen(asset_scope=JobScope.namespace, sweep_id=sweep_id)

    def test_populate_from_scope_record(self):

        scope_id = gen_string_id()
        sweep_id = gen_string_id()

        console_token = 'console token'
        platform_token = 'platform token'

        scope_record = AssetScope()
        scope_record.scope = scope_id
        scope_record.scope_api_token = console_token
        scope_record.set_cache(platform_tokens={platform_token})

        PlatformTokenManager.populate_from_scope_entity(scope_record, sweep_id)

        # now let's make sure we see those tokens:

        # Scope-centered jobs must result in scope-centered key for token storage
        job_scope = JobScope(sweep_id=sweep_id, entity_type=Entity.Scope, entity_id=scope_id)
        assert console_token == PlatformTokenManager.from_job_scope(job_scope).get_best_token()

        job_scope = JobScope(
            sweep_id=sweep_id,
            # uses .namespace default value as 2nd value in redis key. no need to set here.
        )
        assert platform_token == PlatformTokenManager.from_job_scope(job_scope).get_best_token()
