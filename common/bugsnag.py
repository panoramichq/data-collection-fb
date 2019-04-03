"""
Sets up exception handling using Bugsnag
"""
import base64
import bugsnag
import json
import logging
import os
import pickle

from bugsnag.celery import connect_failure_handler
from contextlib import AbstractContextManager

from common.store.base import BaseModel
from common.util import redact_access_token
from config.application import ENVIRONMENT
from config.bugsnag import API_KEY
from config.build import BUILD_ID

_logger = logging.getLogger(__name__)

SEVERITY_ERROR = 'error'
SEVERITY_WARNING = 'warning'


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


class _JSONEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            return super().default(o)
        except:
            try:
                pickle_repr = 'data:application/python-pickle;base64,' + base64.b64encode(pickle.dumps(o)).decode('ascii')
                if isinstance(o, BaseModel):
                    return pickle_repr
                return repr(o) + ';' + pickle_repr
            except:
                return repr(o)


def _make_data_safe_for_serialization(data):
    return json.loads(json.dumps(data, cls=_JSONEncoder))


class BugSnagContextData(AbstractContextManager):
    """
    Context manager allowing passive capture and communication of
    supporting / context data along with the exception when
    exception occurs.

    This allows one to remove try-catch noise from business logic
    and easily instrument the code to communicate extended data

    Example::

        with BugSnagContextData(user_id=123, job_id='fb:123:asdf:4545'):
            do_something_that_may_blow_up(arg, arg)

    """

    def __init__(self, **context_data):
        self.context_data = context_data

    def __enter__(self):
        # allow user to mess around a little more with data collection if they want
        return self.context_data

    @staticmethod
    def notify(exc, severity=SEVERITY_ERROR, **context_data):
        # Serialize errors and send to Bugsnag

        # When we have a top-level exception trapper as well
        # this will effectively cause the exception to be reported twice,
        # once here, with arguments, and again at top level, without
        # the context but with context scraped automatically at the top level
        # For celery tasks this means that same error is reported here,
        # as well as again, just with Celery task arguments.

        # There is a better way...

        # Populate BugSnag's thread-local "session" and add middleware to
        # top-level bugsnag to read that session data and add it to
        # error reporting it does. One error report with all context data

        exc = redact_access_token(exc)

        bugsnag.notify(
            exc,
            meta_data={
                # extra_data will become an "EXTRA DATA" tab in BugSnag
                # json decoder / encoder are (ab)used to do deep
                # but safe serialization of whatever is passed to us as context data.
                'extra_data': _make_data_safe_for_serialization(context_data)
            },
            severity=severity,
        )

    def __exit__(self, exc_type, exc, exc_tb):
        if exc:
            self.notify((exc_type, exc, exc_tb), **self.context_data)
