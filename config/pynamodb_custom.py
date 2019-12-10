# flake8: noqa: E722

# https://github.com/pynamodb/PynamoDB/blob/master/docs/settings.rst
# Overriding defaults to increase connection pool size
# from default 10 to hundreds
# Without this connection setting override, you will see
# botocore.vendored.requests.* emitting:
# "Connection pool is full, discarding connection: dynamo_hostname_here"
# It sounds scary but is actually message from urllib saying that
# it spinned up a connection and used it and was about to put it in a pool, but
# pool is already large so it's throwing it away.

# import logging

# This should be approximately max of total number of
# Write Units + Read Units set on DynamoDB instance in aggregate.
# Since WU and RU are set per table, it's hard to pick specific number
# so we are going with approximate number of concurrent Celery workers
# (the gevent-forked ones per process) as setting for this.
max_pool_connections = 300
