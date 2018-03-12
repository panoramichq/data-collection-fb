import rediscluster
from rediscluster.connection import ClusterConnectionPool

from config import redis as redis_config

def get_redis():
    """
    Obtains a cluster aware redis connection.

    :return ClusterConnectionPool: The redis connection pool
    """

    # Due to a pending release from the `redis-py-cluster` project, pulling in
    # connection code here. Once https://github.com/Grokzen/redis-py-cluster/pull/195
    # is released, this can be reset to the original line (left for ease of transition).

    # return rediscluster.RedisCluster.from_url(redis_config.URL, skip_full_coverage_check=True)

    connection_pool = ClusterConnectionPool.from_url(redis_config.URL, skip_full_coverage_check=True)
    return rediscluster.RedisCluster(connection_pool=connection_pool, skip_full_coverage_check=True)
