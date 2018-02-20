import redis

from config import redis as redis_config


def get_redis():
    return redis.Redis.from_url(redis_config.URL)
