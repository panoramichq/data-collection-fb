import logging
import time

import gevent

from common.celeryapp import CeleryTask
from common.measurement import Measure, CounterMeasuringPrimitive
from config.looper import (
    OOZER_START_RATE,
    OOZER_REVIEW_INTERVAL,
    OOZER_MIN_RATE,
    OOZER_MAX_RATE,
    OOZER_LEARNING_RATE,
    OOZER_ENABLE_LEARNING,
)
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sweep_status_tracker import SweepStatusTracker, Pulse

logger = logging.getLogger(__name__)

OOZING_COUNTER_STEP: int = 100
PULSE_REVIEW_MIN_OOZED_TASKS: int = 100
PULSE_TOTAL_MIN_TASKS: int = 20
PULSE_REVIEW_SUCCESS_THRESHOLD = 0.1
PULSE_REVIEW_THROTTLING_THRESHOLD = 0.4


class TaskOozer:

    sweep_id: str
    sweep_status_tracker: SweepStatusTracker
    pulse_review_interval: int
    stop_oozing_time: float
    wait_interval: int
    oozed_count: int
    oozing_rate: float
    counter: CounterMeasuringPrimitive
    _rate_review_time: int
    _pulse_review_time: int
    _tasks_since_review: int

    def __init__(
        self,
        sweep_id: str,
        sweep_status_tracker: SweepStatusTracker,
        pulse_review_interval: int,
        stop_oozing_time: float,
        *,
        wait_interval: int = 1,
    ):
        self.sweep_id = sweep_id
        self.sweep_status_tracker = sweep_status_tracker
        self.pulse_review_interval = pulse_review_interval
        self.stop_oozing_time = stop_oozing_time
        self.wait_interval = wait_interval
        self.oozed_count = 0
        self.oozing_rate = OOZER_START_RATE
        self.counter = Measure.counter(f'{__name__}.oozed', tags={'sweep_id': sweep_id})
        self._rate_review_time = self._pulse_review_time = round(time.time()) - 1
        self._tasks_since_review = 0

    def __enter__(self) -> 'TaskOozer':
        return self

    def __exit__(self, *args):
        """Perform final counter update."""
        self.counter += self.oozed_count % OOZING_COUNTER_STEP

    @property
    def should_review_oozer_rate(self) -> bool:
        """Oozing rate should be reviewed every X seconds."""
        return self.secs_since_oozer_rate_review >= OOZER_REVIEW_INTERVAL

    @property
    def secs_since_oozer_rate_review(self) -> int:
        """Seconds elapsed since last review."""
        return self.current_time() - self._rate_review_time

    @property
    def expected_tasks_since_oozer_rate_review(self) -> float:
        """Tasks expected to be completed with current rate since review."""
        return self.oozing_rate * self.secs_since_oozer_rate_review

    @staticmethod
    def current_time() -> int:
        return round(time.time()) - 1

    @staticmethod
    def error_function(pulse: Pulse) -> float:
        """Returns error rate [0, 1) used to adapt oozing rate."""
        # + 1 to avoid division by zero
        return pulse.CurrentCounts.UserThrottling / (pulse.CurrentCounts.Total + 1)

    @staticmethod
    def clamp_oozing_rate(rate: float) -> float:
        """Ensure rate between min and max value."""
        return max(OOZER_MIN_RATE, min(rate, OOZER_MAX_RATE))

    @classmethod
    def calculate_rate(cls: 'TaskOozer', current_rate: float, pulse: Pulse) -> float:
        """Calculate new oozing rate based on current rate and oozing pulse."""
        error_rate = cls.error_function(pulse)
        # Larger error rate => larger step
        if error_rate == 0:
            error_rate = -1
        rate_change = -error_rate * OOZER_LEARNING_RATE
        return cls.clamp_oozing_rate(current_rate + (rate_change * current_rate))

    def _ooze_task(self, task: CeleryTask, job_scope: JobScope, job_context: JobContext):
        """Non-blocking task oozing function."""
        task.delay(job_scope, job_context)
        self.oozed_count += 1
        if self.oozed_count % OOZING_COUNTER_STEP == 0:
            self.counter += OOZING_COUNTER_STEP

    def ooze_task(self, task: CeleryTask, job_scope: JobScope, job_context: JobContext):
        """Blocking task oozing function."""
        if OOZER_ENABLE_LEARNING and self.should_review_oozer_rate:
            pulse = self.sweep_status_tracker.get_pulse()
            old_rate = self.oozing_rate
            logger.warning(f'Completed {self._tasks_since_review} tasks in {self.secs_since_oozer_rate_review} seconds')
            self.oozing_rate = self.calculate_rate(old_rate, pulse)
            self._rate_review_time = self.current_time()
            self._tasks_since_review = 0
            logger.warning(f'Updated oozing rate from {old_rate:.2f} to {self.oozing_rate:.2f}')

        if self._tasks_since_review > self.expected_tasks_since_oozer_rate_review:
            gevent.sleep(self.wait_interval)

        self._ooze_task(task, job_scope, job_context)
        self._tasks_since_review += 1

    @property
    def should_review_pulse(self) -> bool:
        """Pulse should be reviewed every X seconds after N tasks have been oozed out."""
        return (
            self.oozed_count >= PULSE_REVIEW_MIN_OOZED_TASKS
            and self.secs_since_pulse_review >= self.pulse_review_interval
        )

    @property
    def secs_since_pulse_review(self) -> int:
        """Seconds elapsed since last review."""
        return self.current_time() - self._pulse_review_time

    def should_terminate(self) -> bool:
        """Whether the oozer should terminate or keep oozing."""
        if self.stop_oozing_time <= time.time():
            pulse = self.sweep_status_tracker.get_pulse()
            logger.warning(
                f'[oozer-run][{self.sweep_id}][breaking-reason] Breaking'
                f' due to running out of time to ooze with pulse: {pulse}'
            )
            return True

        if not self.should_review_pulse:
            return False

        pulse = self.sweep_status_tracker.get_pulse()
        should_terminate = False
        if pulse.Total > PULSE_TOTAL_MIN_TASKS:
            if pulse.Success < PULSE_REVIEW_SUCCESS_THRESHOLD:
                logger.warning(
                    f'[oozer-run][{self.sweep_id}][breaking-reason] Breaking early'
                    f' due to too many failures of any kind (more than 10%) with pulse: {pulse}'
                )
                should_terminate = True
            elif pulse.Throttling > PULSE_REVIEW_THROTTLING_THRESHOLD:
                logger.info(
                    f'[oozer-run][{self.sweep_id}][breaking-reason] Breaking early'
                    f' due to throttling (more than 40%) with pulse: {pulse}'
                )
                should_terminate = True

        self._pulse_review_time = self.current_time()
        return should_terminate
