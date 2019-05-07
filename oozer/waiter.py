import time
import gevent

from common.measurement import Measure, logger, CounterMeasuringPrimitive
from oozer.common.sweep_status_tracker import SweepStatusTracker


class TaskWaiter:

    sweep_id: str
    sweep_status_tracker: SweepStatusTracker
    oozed_total: int
    stop_waiting_time: float
    wait_interval: int
    counter: CounterMeasuringPrimitive

    def __init__(
        self,
        sweep_id: str,
        sweep_status_tracker: SweepStatusTracker,
        oozed_total: int,
        stop_waiting_time: float,
        *,
        wait_interval: int = 1,
    ):
        self.sweep_id = sweep_id
        self.sweep_status_tracker = sweep_status_tracker
        self.oozed_total = oozed_total
        self.stop_waiting_time = stop_waiting_time
        self.wait_interval = wait_interval
        self.counter = Measure.counter(f'{__name__}.done', tags={'sweep_id': sweep_id})
        self._last_total = 0

    def __enter__(self) -> 'TaskWaiter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Perform final counter update."""
        pulse = self.sweep_status_tracker.get_pulse()
        self.counter += pulse.Total - self._last_total

    def should_terminate(self) -> bool:
        """Should we terminate or wait for more tasks to complete."""
        pulse = self.sweep_status_tracker.get_pulse()
        should_be_done_cnt = int(0.9 * self.oozed_total)

        if should_be_done_cnt <= pulse.Total:
            logger.warning(
                f'[oozer-run][{self.sweep_id}][stop-reason] Stopping due to completing >= 90% of oozed tasks'
                f' ({pulse.Total} completed out of {self.oozed_total} oozed) with pulse: {pulse}'
            )
            return True

        if self.stop_waiting_time < time.time():
            logger.warning(
                f'[oozer-run][{self.sweep_id}][stop-reason] Stopping due to running out of time with pulse: {pulse}'
            )
            return True

        return False

    def wait(self):
        """Wait specified wait time while updating the counter."""
        pulse = self.sweep_status_tracker.get_pulse()
        self.counter += pulse.Total - self._last_total
        self._last_total = pulse.Total
        logger.info(f"#{self.sweep_id}: Waiting on {pulse.Total}/{self.oozed_total} jobs with last pulse being {pulse}")
        gevent.sleep(self.wait_interval)
