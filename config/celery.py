# flake8: noqa: E722

# arguments here are new-style (lower-case) Celery config arguments.
# do NOT rename them without checking
# http://docs.celeryproject.org/en/latest/userguide/configuration.html#new-lowercase-settings

broker_url = 'redis://redis'

# Enabling the use of results store (see result_backend = broker_url further below)
result_backend = None
# But, intentionally disabling Celery results store per task
# by default. Enable it on a per-task basis as needed
# by adding `ignore_result=False` to the signature of the Celery task definition.
# (typically it's needed when you are using Celery callbacks / chords)
task_ignore_result = True

# these are overridden in test set up
task_eager_propagates = False
task_always_eager = False

# Make this work for now. If we deem that this is not good enough (TM), we can
# deal with custom serialization logic. This is mainly security concern and
# is problematic when upgrading between pickle protocol versions, if we have
# some tasks in flight. I do believe though that it should be fine right now.
task_serializer = 'pickle'
accept_content = ['application/x-python-serialize', 'application/json']

# controlling how many tasks worker grabs from queue
# default is 4 - too greedy
worker_prefetch_multiplier = 1

# remote control means workers listen on particular queues
# the queues are per workername@hostname and may collide
# with other workers from different builds / deploys
# might be of value to turn this off in prod.
worker_enable_remote_control = True

# we set up our own manually
worker_hijack_root_logger = False

# Disable connection pool
broker_pool_limit = None

# http://docs.celeryproject.org/en/latest/userguide/configuration.html#task-soft-time-limit
# task_soft_time_limit = 60 * 60  # an hour

from common.updatefromenv import update_from_env

update_from_env(__name__)

# for the time being, setting up result store to be same
# thing as message queue - Redis. Note that
# Redis easily chokes if you start throwing large
# amounts of large results through it. If you
# find yourself wanting to use massive Celery chords
# with heavy payloads passing through, switch
# to some real backend. Until then, try to stick to
# some light, payload-less chord orchestration.
result_backend = result_backend or broker_url
