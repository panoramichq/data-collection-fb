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


def run_sweeps_forever():
    while True:
        delay_next_sweep_start_by = run_sweep()
        logger.info(f"Starting next sweep in {delay_next_sweep_start_by} seconds")
        time.sleep(delay_next_sweep_start_by)
