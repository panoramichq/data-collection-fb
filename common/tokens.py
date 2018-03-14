from common.connect.redis import get_redis


class TokenManager:
    token = None

    def __init__(self, asset, sweep_id):
        self.sweep_id = sweep_id
        self.queue_key = f'{asset}-{sweep_id}-sorted-token-queue'

    def add(self, tokens):
        redis = get_redis()
        redis.zadd(self.queue_key, **dict((token, 0) for token in tokens))

    def touch(self, token):
        redis = get_redis()
        redis.zincrby(self.queue_key, token, 1)

    def get_best_token(self):
        redis = get_redis()
        token_candidate = redis.zrange(self.queue_key, 0, 1)[0]
        return token_candidate.decode('utf8')


    def get_token_count(self):
        return get_redis().zcount(self.queue_key, '-inf', '+inf') or 0

