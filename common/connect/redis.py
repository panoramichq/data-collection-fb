import rediscluster

from config import redis as redis_config

def get_redis():
    """
    Obtains a cluster aware redis connection.

    :return ClusterConnectionPool: The redis connection pool
    """

    return rediscluster.RedisCluster.from_url(redis_config.URL)
