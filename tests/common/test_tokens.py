# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.connect.redis import get_redis
from common.tokens import TokenManager


def clean_token_set(token_manager):  # Couldn't find a better way to cleanup redis before tests
    queue_key = token_manager.queue_key
    print(f'Cleaning {queue_key} in redis.')
    redis = get_redis()
    redis.zremrangebyscore(queue_key, '-inf', '+inf')

class TestingTokenManager(TestCase):
    sweep_id = 'test-1'

    def test_empty_state(self):
        token_manager = TokenManager('asset_name', self.sweep_id)
        clean_token_set(token_manager)

        assert token_manager.get_token_count() == 0

    def test_adding_token(self):
        token_manager = TokenManager('asset_name', self.sweep_id)
        clean_token_set(token_manager)
        token = '123'

        token_manager.add([token])
        assert token_manager.get_token_count() == 1
        assert token_manager.get_best_token() == token

    def test_token_priority(self):
        token_manager = TokenManager('asset_name', self.sweep_id)
        clean_token_set(token_manager)

        first_token = 'first'
        second_token = 'second'

        token_manager.add([first_token, second_token])

        assert token_manager.get_token_count() == 2

        # Used the first token
        token_manager.touch(first_token)

        assert token_manager.get_best_token() == second_token

