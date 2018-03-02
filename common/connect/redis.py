from config import redis as redis_config


def get_redis():
    """
    Obtains either standard redis connection or cluster aware connection. These
    are mutually exclusive. Using a simple flag, we can potentially add some
    auto-sniff-cluster functionality if we want to, won't be that hard

    :return ConnectionPool | ClusterConnectionPool: The redis connection pool
    """
    if redis_config.CLUSTER:
        import rediscluster
        return rediscluster.RedisCluster.from_url(redis_config.URL)
    else:
        import redis
        return redis.Redis.from_url(redis_config.URL)
