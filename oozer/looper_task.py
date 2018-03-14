import logging
import time

from typing import Tuple

from common.celeryapp import get_celery_app, RoutingKey
from common.measurement import Measure

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task(routing_key=RoutingKey.longrunning)
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def sweep_looper_task(sweep_id):
    from oozer.looper import run_tasks, Pulse
    from sweep_builder.tasks import sweep_builder_task
    from config import looper as looper_config

    min_sweep_seconds = looper_config.FB_THROTTLING_WINDOW
    sweep_start = time.time()

    logger.info(f"#{sweep_id}: Starting sweep loop")
    cnt, pulse = run_tasks(sweep_id)  # type: Tuple[int, Pulse]
    logger.info(f"#{sweep_id}: Ran {cnt} total jobs with following outcomes: {pulse}")

    seconds_left_in_the_sweep = time.time() - (sweep_start + min_sweep_seconds)
    throttling_mini_reset_seconds = 60
    if seconds_left_in_the_sweep > throttling_mini_reset_seconds:
        # We are done with sweep a bit early.
        # There is really no good reason to have us
        # scan Facebook too frequently, as no amount
        # of recentness will bring us closer to real-time
        # and we'll just clobber throttling limits with
        # tasks that are just too highly-scored on EVERY sweep
        # like Entities scans
        # However, in early seed sweeps it'd be nice to sweep repeatedly
        # to build up the reality universe quickly
        # seed sweeps are usually ones with very few number of tasks. So:
        if cnt < 300: # arbitrary "small" number of tasks
            delay_next_sweep_start_by = pulse.Throttling * throttling_mini_reset_seconds
        else:
            delay_next_sweep_start_by = pulse.Throttling * seconds_left_in_the_sweep
    else:
        # we are beyond the *Minimum* sweep time.
        # Let's start another one.
        # It's nice to give about a minute to let the throttling counter to
        # start easing before queueing next one, but
        # if ratio of throttling errors is low, why wait?
        delay_next_sweep_start_by = pulse.Throttling * throttling_mini_reset_seconds

    if looper_config.ALLOW_RECURSION:
        logger.info(f"#{sweep_id}: Starting next sweep in {delay_next_sweep_start_by} seconds")
        sweep_builder_task.apply_async(
            (),  # args to the task - none
            countdown=delay_next_sweep_start_by
        )
    else:
        logger.info(f"#{sweep_id}: End of all work")
