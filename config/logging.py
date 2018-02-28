import logging

LEVEL = 'WARNING'

from common.updatefromenv import update_from_env
update_from_env(__name__)

if isinstance(LEVEL, str):
    # they must be native logging enums (ints)
    LEVEL = logging.getLevelName(LEVEL)