from celery import Celery
from celery.signals import setup_logging

from config import celery as celery_config


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
    pass


_celery_app = None


def get_celery_app(celery_config=celery_config):
    global _celery_app

    if not _celery_app:
        _celery_app = Celery()
        _celery_app.config_from_object(celery_config)
        _celery_app.autodiscover_tasks(
            [
                'prioritizer',
                'oozer'
            ]
        )

    return _celery_app
