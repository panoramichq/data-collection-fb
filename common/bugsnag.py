"""
Sets up exception handling using Bugsnag
"""
import os
import logging

import bugsnag
from bugsnag.celery import connect_failure_handler

from config.application import ENVIRONMENT
from config.bugsnag import API_KEY
from config.build import BUILD_ID

_logger = logging.getLogger(__name__)


def configure_bugsnag():
    """
    Hooks up Bugsnag to Celery. You should do this early in the startup process
    to make sure we are setup correctly from the beginning.
    """
    if not API_KEY:
        _logger.warning(
            "Bugsnag API key is not set, cannot configure exception tracking"
        )
        return

    bugsnag.configure(
        api_key=API_KEY,
        project_root=os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
        app_version=BUILD_ID,
        release_stage=ENVIRONMENT
    )

    # register a handler to capture celery errors
    connect_failure_handler()
