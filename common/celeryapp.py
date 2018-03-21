from celery import Celery, Task
from celery.signals import setup_logging

from config import celery as celery_config
from config.build import BUILD_ID
from common.configure_logging import configure_logging
from common.bugsnag import configure_bugsnag


MODULES_WITH_TASKS = [
    'oozer',
    'oozer.common',
    'oozer.entities',
    'oozer.metrics',
    'sweep_builder',
]


class RoutingKey:

    default = 'default'
    longrunning = 'longrunning'

    ALL = {
        default,
        longrunning
    }


class CeleryTask(Task):

    ignore_result = True
    max_retries = 0


@setup_logging.connect
def _alter_logger(*args, **kwargs):
    """
    Celery replaces root logger by default with its own crooked version.
    Just using the @setup_logging.connect callback causes Celery NOT to override
    the root logger

    http://docs.celeryproject.org/en/latest/userguide/signals.html#setup-logging

    So, while having this callback defined is good for keeping Celery away from
    root logger, it's also a good place to set up logger we want.
    """

    # TODO: dress up root logger here under Celery
    # configure_logging()
    pass


_celery_app = None


def pad_with_build_id(base_name):
    # because we could share the same instance of Redis for multiple
    # parallel versions of this stack,
    # we need to separate the queues of one stack version from another.
    # Thus, we are injecting the build ID into the queue name to separate our
    # queues from queues of siblings.
    return f'{base_name}-{BUILD_ID}'


def get_celery_app(celery_config=celery_config):
    global _celery_app

    if not _celery_app:
        _celery_app = Celery(
            task_cls=CeleryTask
        )
        _celery_app.config_from_object(celery_config)

        # These are top-level module names for folders
        # within which we may have files names `tasks.py`
        # that auto-dicsoverer will find and scrape Celery tasks from
        _celery_app.autodiscover_tasks(
            MODULES_WITH_TASKS
        )

        # The concurrency used by individual celery process (per our settings)
        # is Gevent-based concurrency, which is very brittle to non-cooperative threads
        # THus, we have a need to separate long-running (non-cooperative, blocking IO) tasks
        # into separate celery process.
        # Typically that is done by channeling specific tasks into specific queues
        # and having completely separate celery worker processes watch these separate queues.
        # The approach taken below is to herd very cooperative tasks (those that yield on IO) into
        # `default` queue and potentially-long-running processes into `longrunning` queue.

        # Note that while we are passing queue names here, we don't want
        # to change the routing hints elsewhere in the code.
        # In the code, tasks use constant routing_key values that map
        # to dynamically-generated queue names here.

        special_task_routes = {
            # routing_key: queue name
            RoutingKey.longrunning: pad_with_build_id(RoutingKey.longrunning),
        }

        # This is manual router implementation for Celery, per spec detailed here:
        # http://docs.celeryproject.org/en/latest/userguide/configuration.html#task-routes
        # The idea is to avoid static routing table and, instead,
        # route to special dynamically-named queues based on special routing keys and
        # fall back to default queue otherwise
        # Partially purposefully verbose about it to make link to
        # use of routing_key enum so very explicit.

        def route_task(name, args, kwargs, options, task=None, **kw):
            routing_key = options.get('routing_key', RoutingKey.default)
            if routing_key in special_task_routes:
                return {
                    'queue': special_task_routes[routing_key]
                }
            else:
                # will result in this task falling into default route and queue
                return {}

        _celery_app.conf.task_default_queue = pad_with_build_id(RoutingKey.default)
        _celery_app.conf.task_default_routing_key = RoutingKey.default
        _celery_app.conf.task_routes = (route_task,)

        # Enable Bugsnag exception tracking
        configure_bugsnag()

    return _celery_app
