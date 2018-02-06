# arguments here are new-style (lower-case) Celery config arguments.
# do NOT rename them without checking
# http://docs.celeryproject.org/en/latest/userguide/configuration.html#new-lowercase-settings

broker_url = 'redis://redis'
# ny default, result storage backend is disabled
# must set the value to something to enable it.
# for now, the same store (and url) as the broker will do
result_backend = broker_url

from common.updatefromenv import update_from_env
update_from_env(__name__)
