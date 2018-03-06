# Bear in mind this URL is a redis cluster instance. This will not work with
# normal redis at all

# When using AWS ElastiCache connect only to the "Configuration Endpoint" as
# visible in the AWS console (hence we don't need multiple host addresses)
URL = 'redis://redis:6379'


from common.updatefromenv import update_from_env
update_from_env(__name__)
