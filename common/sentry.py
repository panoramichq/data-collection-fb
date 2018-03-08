"""
Sets up exception handling using the Sentry system
"""
import logging

from raven import Client
from raven.contrib.celery import register_signal, register_logger_signal

from config import sentry, build

_logger = logging.getLogger(__name__)


def configure_sentry():
    """
    Hooks up Sentry to Celery. You should do this early in the startup process
    to make sure we are setup correctly from the beginning.
    """
    if not sentry.DSN:
        _logger.warning(
            "Sentry DSN is not set, cannot configure exception tracking"
        )
        return

    client = Client(
        dsn=sentry.DSN,
        release=build.BUILD_ID
    )

    # register a custom filter to filter out duplicate logs
    register_logger_signal(client)

    # The register_logger_signal function can also take an optional argument
    # `loglevel` which is the level used for the handler created.
    # Defaults to `logging.ERROR`
    register_logger_signal(client, loglevel=sentry.CAPTURE_LEVEL)

    # hook into the Celery error handler
    register_signal(client)

    # The register_signal function can also take an optional argument
    # `ignore_expected` which causes exception classes specified in Task.throws
    # to be ignored
    register_signal(client, ignore_expected=True)
