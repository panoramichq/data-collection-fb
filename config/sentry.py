import logging

# The Sentry DSN for capturing errors
DSN = None

CAPTURE_LEVEL = 'INFO'

from common.updatefromenv import update_from_env
update_from_env(__name__)

if isinstance(CAPTURE_LEVEL, str):
    # they must be native logging enums (ints)
    CAPTURE_LEVEL = logging.getLevelName(CAPTURE_LEVEL)
