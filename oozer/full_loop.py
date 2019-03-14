"""
After playing with various ways to recover from failures in main loop
failures in data collection system, Mike, Daniel decided to switch from
distributed loop (2 separate Celery tasks - sweep builder and sweep runner
calling each other recursively) to an in-process, always-resident loop that
cycles between builder and task looper functions.

The value of single-process builder+looper is in our ability to know exactly
if it's running or not and rescheduling another instance of it automatically
when / if it dies.

This outer loop does NOT do much (IO or number-crunching). It's designed to
fan out celery tasks and quietly wait on them. Both, Sweep Builder and Sweep Looper
tasks are, effectively creating, fanning out tasks and waiting on them in a recursive loop.
"""
import logging
import time
from datetime import datetime

from common.measurement import Measure
from oozer.looper import run_sweep_looper_suggest_restart_time
from sweep_builder.tasks import build_sweep

logger = logging.getLogger(__name__)


def generate_sweep_id():
    return datetime.utcnow().strftime('%Y%m%d%H%M%S')


def run_sweep(sweep_id=None):
    sweep_id = sweep_id or generate_sweep_id()
    build_sweep(sweep_id)
    delay_next_sweep_start_by = run_sweep_looper_suggest_restart_time(sweep_id)
    return delay_next_sweep_start_by


def run_sweep_and_sleep(sweep_id=None):
    """
    Like run_sweep but actually sleeps for suggested amount of time before quitting.

    This is used to internalize the management of period between consecutive sweep runs.
    This is a crude way to spacing out the sweep runs. Alternative would be to
    turn runner back into a Celery task and use Celery timed delay API for recursive
    self-scheduling.

    :param sweep_id:
    :return:
    """

    delay_next_sweep_start_by = run_sweep(sweep_id=sweep_id)
    _measurement_name_base = __name__ + '.run_sweep_and_sleep.'  # <- function name. adjust if changed
    _measurement_tags = dict(
        sweep_id=sweep_id
    )
    Measure.gauge(_measurement_name_base + 'delay_next_sweep_start_by', tags=_measurement_tags)(int(delay_next_sweep_start_by))
    logger.info(f"Done with main sweep run. Waiting for {delay_next_sweep_start_by} seconds before quitting")
    time.sleep(delay_next_sweep_start_by)


def run_sweeps_forever():
    while True:
        run_sweep_and_sleep()
