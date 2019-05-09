import logging
import time

from typing import Tuple

from common.measurement import Measure
from common.timeout import timeout
from config import looper as looper_config
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.common.sweep_status_tracker import SweepStatusTracker, Pulse
from oozer.oozer import TaskOozer
from oozer.producer import TaskProducer
from oozer.waiter import TaskWaiter

logger = logging.getLogger(__name__)


@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@timeout(looper_config.RUN_TASKS_TIMEOUT)
def run_tasks(sweep_id: str) -> Tuple[int, Pulse]:
    """Oozes tasks gradually into Celery workers queue."""
    start_time = time.time()
    stop_waiting_time = start_time + looper_config.FB_THROTTLING_WINDOW
    # 90% time oozing; 10% waiting
    stop_oozing_time = start_time + 0.9 * looper_config.FB_THROTTLING_WINDOW

    pulse_review_interval = 5  # seconds
    sweep_tracker = SweepStatusTracker(sweep_id)
    sweep_tracker.start_metrics_collector(pulse_review_interval)

    producer = TaskProducer(sweep_id)
    num_accounts = producer.get_ad_account_count()
    num_tasks = producer.get_task_count()

    logger.warning(
        f'[oozer-run][{sweep_id}][initial-state] Starting oozer '
        f'with {num_tasks} scheduled tasks for {num_accounts} accounts'
    )

    last_score = None
    with TaskOozer(sweep_id, sweep_tracker, pulse_review_interval, stop_oozing_time) as oozer:
        for celery_task, job_scope, job_context, score in producer.iter_tasks():
            last_score = score
            if oozer.should_terminate():
                break
            oozer.ooze_task(celery_task, job_scope, job_context)

    oozed_count = oozer.oozed_count

    pulse = sweep_tracker.get_pulse()
    logger.warning(
        f'[oozer-run][{sweep_id}][oozing-done]: Oozed out {oozed_count} tasks out of {num_tasks}'
        f' with pulse: {pulse} and last score {last_score}'
    )

    logger.warning(
        f'[oozer-run][{sweep_id}][waiting] Starting waiter'
        f' with {oozed_count} tasks oozed and waiting til {stop_waiting_time}'
    )

    with TaskWaiter(sweep_id, sweep_tracker, oozed_count, stop_waiting_time) as waiter:
        while not waiter.should_terminate():
            waiter.wait()

    pulse = sweep_tracker.get_pulse()
    logger.warning(f'[oozer-run][{sweep_id}][exiting] Exited oozer with pulse: {pulse}')

    return oozed_count, pulse


@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def run_sweep_looper_suggest_restart_time(sweep_id: str) -> int:
    sweep_start = time.time()

    logger.info(f"#{sweep_id}: Starting sweep loop")
    with SweepRunningFlag(sweep_id):
        cnt, pulse = run_tasks(sweep_id)
    logger.info(f"#{sweep_id}: Ran {cnt} total jobs with following outcomes: {pulse}")

    min_sweep_seconds = looper_config.FB_THROTTLING_WINDOW
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
        if cnt < 300:  # arbitrary "small" number of tasks
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

    logger.warning(f'#{sweep_id}: Delaying next sweep run by: {delay_next_sweep_start_by} seconds')

    return delay_next_sweep_start_by
