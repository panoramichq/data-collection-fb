# flake8: noqa: E722

RUN_SWEEP_TIMEOUT = 60 * 60  # seconds
RUN_TASKS_TIMEOUT = 30 * 60  # 30 minutes.

FB_THROTTLING_WINDOW = 20 * 60  # 20 minutes.

OOZER_ENABLE_LEARNING = True
OOZER_LEARNING_RATE = 0.05
OOZER_REVIEW_INTERVAL = 10
OOZER_START_RATE = 100.0
OOZER_MIN_RATE = 10.0

# Hour, DMA, AgeGender * about 2 years back + 3 levels of lifetime + 3 levels of Entities
_number_of_long_tasks_per_aa = 3 * 600 + 3 + 3

# there is also a way to get to this real number by scanning FacebookAdAccount table,
# but, we'd like to dream.
# This does not really limit anything, just defines some optimistic upper range for
# "normal" when it comes estimating "total possible" number of tasks to run per sweep.
MAX_EXPECTED_AD_ACCOUNTS = 500  # yeah, optimistic

# Real number of tasks can be much greater, as we are not counting
# per-entity report type permutations. There there can be millions per single AdAccount
# for that reason:
_looney_balooney_multiplier = 10  # yeah, this is NOT millions, but something, right
# When we relax prioritizer logic to schedule per-entity tasks along with per-parent
# tasks, you need to crank this ^ up quite a bit, to 10,000 at least.

# Used to define "max normal" estimatable tasks when it comes to Early Exit logic in looper
SANE_MAX_TASKS = _number_of_long_tasks_per_aa * MAX_EXPECTED_AD_ACCOUNTS * _looney_balooney_multiplier

from common.updatefromenv import update_from_env

update_from_env(__name__)
