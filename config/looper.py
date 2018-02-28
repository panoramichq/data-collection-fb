FB_THROTTLING_WINDOW = 10*60  # seconds
DECAY_FN_START_MULTIPLIER = 3  # value of 'z' used in linear decay formulas

from common.updatefromenv import update_from_env
update_from_env(__name__)
