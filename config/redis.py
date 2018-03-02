URL = 'redis://redis:6379'

# If you use this setting, the non-Celery redis will connect to a redis cluster
# as opposed to a normal Redis.
# As these drivers are not compatible with each other, use against an actual
# cluster only.
#
# When using AWS ElastiCache connect only to the "Configuration Endpoint" as
# visible in the AWS console (hence we don't need multiple host addresses)
CLUSTER = False


from common.updatefromenv import update_from_env
update_from_env(__name__)
