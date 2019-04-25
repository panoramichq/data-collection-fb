import gevent
import logging
import time

from itertools import islice
from typing import Generator, Tuple, Any

from common.celeryapp import CeleryTask
from common.id_tools import parse_id
from common.math import adapt_decay_rate_to_population, get_decay_proportion
from common.measurement import Measure
from common.timeout import timeout
from config import looper as looper_config
from config.looper import (
    OOZER_REVIEW_INTERVAL,
    OOZER_MAX_RATE,
    OOZER_MIN_RATE,
    OOZER_LEARNING_RATE,
    OOZER_ENABLE_LEARNING,
    OOZER_START_RATE,
)
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.common.sweep_status_tracker import SweepStatusTracker, Pulse
from oozer.inventory import resolve_job_scope_to_celery_task

logger = logging.getLogger(__name__)


OOZER_CUT_OFF_SCORE = 2


def iter_tasks(sweep_id: str) -> Generator[Tuple[CeleryTask, JobScope, JobContext, int], None, None]:
    """
    Persist prioritized jobs and pass-through context objects for inspection
    """
    with SortedJobsQueue(sweep_id).JobsReader() as jobs_iter:
        for job_id, job_scope_additional_data, score in jobs_iter:

            job_id_parts = parse_id(job_id)
            job_scope = JobScope(job_scope_additional_data, job_id_parts, sweep_id=sweep_id, score=score)

            celery_task = resolve_job_scope_to_celery_task(job_scope)

            if not celery_task:
                logger.warning(f"#{sweep_id}: Could not match job_id {job_id} to a worker.")
            else:
                # TODO: Decide what to do with this.
                # Was designed for massive hash collection and such,
                # but cannot have too much data in there because we pickle it and put in on Redis
                job_context = JobContext()

                yield celery_task, job_scope, job_context, score

                logger.info(f"#{sweep_id}: Scheduling job_id {job_id} with score {score}.")


class AdaptiveTaskOozer:

    sweep_status_tracker: SweepStatusTracker
    actual_processed: int
    start_time: int
    rate_review_time: int
    tasks_since_review: int
    wait_time: int
    oozing_rate: float

    def __init__(self, sweep_status_tracker: SweepStatusTracker, wait_time: int = 1):
        self.sweep_status_tracker = sweep_status_tracker
        self.actual_processed = 0
        self.start_time = self.rate_review_time = round(time.time()) - 1
        self.tasks_since_review = 0
        self.wait_time = wait_time
        self.oozing_rate = OOZER_START_RATE

    def __enter__(self):
        return self.ooze_task

    def __exit__(self, *args):
        # kill outstanding tasks?
        pass

    @property
    def should_review_rate(self) -> bool:
        """Oozing rate should be reviewed every X seconds."""
        return self.current_time() - self.rate_review_time >= OOZER_REVIEW_INTERVAL

    @property
    def secs_since_review(self) -> int:
        """Seconds elapsed since last review."""
        return self.current_time() - self.rate_review_time

    @property
    def normative_tasks_since_review(self) -> float:
        """Tasks expected to be completed with current rate since review."""
        return self.oozing_rate * self.secs_since_review

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
    def calculate_rate(cls: 'AdaptiveTaskOozer', current_rate: float, pulse: Pulse) -> float:
        """Calculate new oozing rate based on current rate and oozing pulse."""
        error_rate = cls.error_function(pulse)
        # Larger error rate => larger step
        if error_rate == 0:
            error_rate = -1
        rate_change = -error_rate * OOZER_LEARNING_RATE
        return cls.clamp_oozing_rate(current_rate + (rate_change * current_rate))

    def ooze_task(self, task: CeleryTask, job_scope: JobScope, job_context: JobContext, *_: Any, **__: Any):
        """Tracks the number of calls"""
        if OOZER_ENABLE_LEARNING and self.should_review_rate:
            pulse = self.sweep_status_tracker.get_pulse()
            old_rate = self.oozing_rate
            logger.warning(f'Completed {self.tasks_since_review} tasks in {self.secs_since_review} seconds')
            self.oozing_rate = self.calculate_rate(old_rate, pulse)
            self.rate_review_time = round(time.time()) - 1
            self.tasks_since_review = 0
            logger.warning(f'Updated oozing rate from {old_rate:.2f} to {self.oozing_rate:.2f}')

        if self.tasks_since_review > self.normative_tasks_since_review:
            gevent.sleep(self.wait_time)

        task.delay(job_scope, job_context)
        self.tasks_since_review += 1
        self.actual_processed += 1

        return True


@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@timeout(looper_config.RUN_TASKS_TIMEOUT)
def run_tasks(
    sweep_id: str, limit: int = None, time_slices: int = looper_config.FB_THROTTLING_WINDOW, time_slice_length: int = 1
) -> Tuple[int, Pulse]:
    """
    Oozes tasks gradually into Celery workers queue, accounting for total number of tasks
    and the window of time over which we want them to be processed.

    :param sweep_id: Current sweep
    :param limit: Max number of tasks to push out.
        Can also be a shortcut from calling code indicating total size of population,
        allowing us to avoid checking the size of collection on disk - time savings.
    :param time_slices: Number of one-second periods to spread the tasks over
    :param time_slice_length: in seconds. can be fractional
    """
    _measurement_name_base = f'{__name__}.{run_tasks.__name__}.'
    _measurement_tags = {'sweep_id': sweep_id}
    _step = 100

    num_accounts = SortedJobsQueue(sweep_id).get_ad_accounts_count()
    num_tasks = limit or SortedJobsQueue(sweep_id).get_queue_length()

    start_of_run_seconds = time.time()
    max_normal_running_time_seconds = time_slices * time_slice_length
    max_seed_running_time_seconds = max_normal_running_time_seconds * 1.3  # arbitrary
    quarter_time = start_of_run_seconds + max_normal_running_time_seconds * 0.25
    half_time = start_of_run_seconds + max_normal_running_time_seconds * 0.5

    cnt = 0
    _pulse_refresh_interval = 5  # seconds
    sweep_tracker = SweepStatusTracker(sweep_id)
    sweep_tracker.start_metrics_collector(_pulse_refresh_interval)
    last_processed_score = None

    tasks_iter = iter_tasks(sweep_id)
    if limit:
        tasks_iter = islice(tasks_iter, 0, limit)

    def task_iter_score_gate(inner_tasks_iter):
        for inner_celery_task, inner_job_scope, inner_job_context, inner_score in inner_tasks_iter:
            if inner_score < OOZER_CUT_OFF_SCORE:  # arbitrary
                # cut the flow of tasks
                return

            # We need to see into the jobs scoring state per sweep
            additional_tags = {
                'sweep_id': sweep_id,
                'score': inner_score,
                'ad_account_id': inner_job_scope.ad_account_id,
                'report_type': inner_job_scope.report_type,
                'report_variant': inner_job_scope.report_variant,
                'job_type': inner_job_scope.job_type,
            }
            Measure.counter(
                _measurement_name_base + 'job_scores', tags={**additional_tags, **_measurement_tags}
            ).increment()

            yield inner_celery_task, inner_job_scope, inner_job_context, inner_score

    tasks_iter = task_iter_score_gate(tasks_iter)

    logger.warning(
        f'[oozer-run][{sweep_id}][initial-state] Starting oozer '
        f'with {num_tasks} scheduled tasks for {num_accounts} accounts'
    )
    with AdaptiveTaskOozer(sweep_tracker, wait_time=time_slice_length) as ooze_task, Measure.counter(
        _measurement_name_base + 'oozed', tags=_measurement_tags
    ) as cntr:
        keep_going = True
        next_pulse_review_second = time.time() + _pulse_refresh_interval

        # now, don't freak out about us looping 4 time off the same exact generator
        # the beauty of generator is that you can resume consuming from it
        # as each loop is just asking g.__next__() on it. So, we can part-consume from it
        # reflect on what we did and continue looping through for bit, stop and
        # reflect a bit more etc etc.

        # first some n jobs, it's kinda pointless checking on pulse.
        # let's just shove them out of the door without
        # thinking about pulse or failures.
        # This makes sure that seed rounds that have so few
        # tasks get all COMPLETELY pushed out.
        for celery_task, job_scope, job_context, score in islice(tasks_iter, 0, 100):
            # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
            # It will unblock by itself when it's time to release the task
            keep_going = ooze_task(celery_task, job_scope, job_context, sweep_id, last_processed_score)

            if keep_going:
                last_processed_score = score
            else:
                logger.warning(
                    f'[oozer-run][{sweep_id}][breaking-reason] Breaking very early without checking pulse '
                    f'with following pulse: {sweep_tracker.get_pulse()} at minute {sweep_tracker.now_in_minutes()}'
                    f' and last score {last_processed_score}'
                )
                break
            cnt += 1

        if keep_going:
            cntr += cnt

            # if there are 1st quarter tasks left in queue, burn them out
            for celery_task, job_scope, job_context, score in tasks_iter:
                keep_going = ooze_task(celery_task, job_scope, job_context, sweep_id, last_processed_score)
                if keep_going:
                    cnt += 1
                    last_processed_score = score

                    if cnt % _step == 0:
                        cntr += _step

                if time.time() > quarter_time or not keep_going:
                    logger.warning(
                        f'[oozer-run][{sweep_id}][breaking-reason] Breaking early in 1st quarter time, '
                        f'I am too slow {time.time()} / {quarter_time}'
                        f' with following pulse: {sweep_tracker.get_pulse()} at minute {sweep_tracker.now_in_minutes()}'
                        f' and last score {last_processed_score}'
                    )
                    break  # to next for-loop

        if keep_going:
            for celery_task, job_scope, job_context, score in tasks_iter:
                # If we are here, it's start of 2nd quarter of our total time slice
                # and we still have tasks to push out.
                # At this point we should start caring about results
                # We will look for most obvious signs of failure

                now = time.time()
                if next_pulse_review_second < now:
                    pulse = sweep_tracker.get_pulse()
                    if pulse.Total > 20:
                        if pulse.Success < 0.10:  # percent
                            # failures across the board
                            # return cnt, pulse
                            logger.warning(
                                f'[oozer-run][{sweep_id}][breaking-reason] Breaking early in 2nd quarter time, '
                                'due to too many failures of any kind '
                                f'(more than 10 percent) with following pulse: {sweep_tracker.get_pulse()}'
                                f' at minute {sweep_tracker.now_in_minutes()}'
                                f' and last score {last_processed_score}'
                            )
                            break
                        if pulse.Throttling > 0.40:  # percent
                            # time to give it a rest
                            # return cnt, pulse
                            logger.info(
                                f'[oozer-run][{sweep_id}][breaking-reason] Breaking early in 2nd quarter time, '
                                'due to throttling (more than 40 percent)'
                                f' with following pulse: {sweep_tracker.get_pulse()}'
                                f' at minute {sweep_tracker.now_in_minutes()}'
                                f' and last score {last_processed_score}'
                            )
                            break
                    next_pulse_review_second = now + _pulse_refresh_interval

                # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
                # It will unblock by itself when it's time to release the task
                keep_going = ooze_task(celery_task, job_scope, job_context, sweep_id, last_processed_score)
                if keep_going:
                    cnt += 1
                    last_processed_score = score

                    if cnt % _step == 0:
                        cntr += _step

                    if now > half_time:
                        logger.info(
                            f'[oozer-run][{sweep_id}][breaking-reason] Breaking early in 2nd quarter time, '
                            f'I am too slow {now}/{half_time} with following pulse: {sweep_tracker.get_pulse()}'
                            f' at minute {sweep_tracker.now_in_minutes()}'
                            f' and last score {last_processed_score}'
                        )
                        break
                else:
                    break

        # In second half of the loop, if we still have tasks to release
        # we might need to cut the tail of the task queue if we now realize we will not
        # burn through it all.
        # For that we need this:
        pulse = sweep_tracker.get_pulse()
        half_time_done_cnt = pulse.Total
        # Note, that we may be here in the first second of the sweep loop if we have
        # some very few seed tasks. So, this is NOT guaranteed to be exactly the middle
        # of the sweep loop as far as time is concerned. This could be "the end" of the loop
        # as far as tasks oozing is concerned and "the very beginning" of the loop in terms of time.
        # this value is "half_time" only if the next for loop has any items to process.
        # So, don't do any "half" logic here. Do it inside this next for loop.
        cut_off_at_cnt = num_tasks

        if keep_going:
            for celery_task, job_scope, job_context, score in tasks_iter:
                # If we are here, exactly start of 2nd half of our total time slice
                # and we still have tasks to push out.

                # since we need only one swing through the loop - just to ensure
                # there is something there,
                # could have done tasks_iter.__next__() wrapped in try-catch, but
                # did not want to break the pattern.

                cut_off_at_cnt = min(cut_off_at_cnt, half_time_done_cnt * 2)

                keep_going = ooze_task(celery_task, job_scope, job_context, sweep_id, last_processed_score)
                if keep_going:
                    cnt += 1
                    last_processed_score = score

                    if cnt % _step == 0:
                        cntr += _step

                break

        if keep_going:
            for celery_task, job_scope, job_context, score in tasks_iter:
                # If we are here, we are a little bit into 2nd half of our total time slice
                # and we still have tasks to push out.

                # At this point we should start caring about what we queue up,
                # because by about half-time we need to know if we cut the cycle or not
                # Here we are building linear equation derived from observing rate and speed of
                # completion of the tasks we already pushed out and trying to predict
                # what happens to jobs we would push out from this point on.

                now = time.time()

                if next_pulse_review_second < now:
                    pulse = sweep_tracker.get_pulse()
                    if pulse.Total > 20:
                        if pulse.Success < 0.20:  # percent
                            # failures across the board
                            # return cnt, pulse
                            logger.warning(
                                f'[oozer-run][{sweep_id}][breaking-reason] Breaking 2nd halif time, '
                                'due to too many failures of any kind '
                                f'(more than 20 percent) with following pulse: {pulse}'
                                f' at minute {sweep_tracker.now_in_minutes()}'
                                f' and last score {last_processed_score}'
                            )
                            break
                        if pulse.Throttling > 0.40:  # percent
                            # time to give it a rest
                            # return cnt, pulse
                            logger.warning(
                                f'[oozer-run][{sweep_id}][breaking-reason] Breaking early in 2nd quarter time, '
                                'due to throttling (more than 40 percent)'
                                f' with following pulse: {pulse}'
                                f' and last score {last_processed_score}'
                            )
                            break
                    next_pulse_review_second = time.time() + _pulse_refresh_interval

                # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
                # It will unblock by itself when it's time to release the task
                keep_going = ooze_task(celery_task, job_scope, job_context, sweep_id, last_processed_score)
                if keep_going:
                    cnt += 1
                    last_processed_score = score

                    if cnt % _step == 0:
                        cntr += _step

                    if cnt > cut_off_at_cnt:
                        if cnt < num_tasks:
                            logger.warning(f"#{sweep_id}: Queueing cut at {cnt} jobs of total {num_tasks}")
                        logger.warning(
                            f'[oozer-run][{sweep_id}][breaking-reason][{sweep_id}] Breaking early'
                            ' due to reaching limit on tasks'
                            f' with following pulse: {sweep_tracker.get_pulse()}'
                            f' at minute {sweep_tracker.now_in_minutes()}'
                            f' and last score {last_processed_score}'
                        )
                        break
                else:
                    logger.warning(
                        f'[oozer-run][{sweep_id}][breaking-reason][{sweep_id}] Breaking early '
                        'due to problem with oozing in the last phase'
                        f' with following pulse: {sweep_tracker.get_pulse()}'
                        f' at minute {sweep_tracker.now_in_minutes()}'
                        f' and last score {last_processed_score}'
                    )
                    break

            logger.warning(f"#{sweep_id}: Queued up all jobs {num_tasks}")

    cntr += cnt % _step

    logger.info(f"#{sweep_id}: Queued up {cnt} jobs")

    # now we wait for either the time to run out or tasks to be done
    # This part is icky. Here all we do is wait. One of the things we wait on - done count -
    # is not guaranteed to be there ever. If we implemented error trapping in workers badly,
    # then we'll wait forever... but that's why we also have time-based cut off, but
    # that basically means that a single uncaught error in small sweeps (see under-500 logic below)
    # results in this function running for at least 10 minutes
    # (or whatever time_slices * time_slice_length is)
    #
    # TODO: split this off into separate waiter code/function from oozer code/function above.
    #  (would do that myself but there are too many variables shared now... too much worky)

    # arbitrary, but just has to be some "small" value
    # that indicates this is one of earlier, "seed" sweeps
    # where collection of stuff is super important

    if cnt > 100:
        should_be_done_cnt = int(cnt * 0.90)  # 90% is good enough
        should_be_done_by = start_of_run_seconds + max_seed_running_time_seconds
        _rate = adapt_decay_rate_to_population(max(cnt * 2, looper_config.SANE_MAX_TASKS))
        # this first picks a cnt-appropriate ratio between 1 and 0,
        # depending on value of cnt. The closer it is to SANE_MAX_TASKS
        # the *smaller* (closer to zero) that ratio is.
        # Then expectation of "not even caring about the clock" is set
        # based on cnt * that ratio
        dont_even_look_at_clock_until_done_cnt = int(cnt * get_decay_proportion(cnt, rate=_rate))
    else:
        should_be_done_cnt = cnt
        should_be_done_by = start_of_run_seconds + max_normal_running_time_seconds
        dont_even_look_at_clock_until_done_cnt = cnt

    really_really_kill_it_by = max(should_be_done_by, start_of_run_seconds + 60 * 30)  # half hour

    def its_time_to_quit(inner_pulse: Pulse) -> bool:
        # must have anything at all done
        # otherwise logic below makes no sense
        # This stops us from quitting in very beginning of the loop
        # global dont_even_look_at_clock_until_done_cnt, really_really_kill_it_by, should_be_done_cnt

        if should_be_done_cnt <= inner_pulse.Total:
            # Yey! all done!
            logger.warning(
                f'[oozer-run][{sweep_id}][stop-reason] Stopping due to completing 90 % of oozed tasks '
                f' ({should_be_done_cnt} oozed out of {num_tasks} scheduled)'
                f' with following pulse: {inner_pulse}'
                f' and last score {last_processed_score}'
            )
            return True

        if inner_pulse.Total:
            # all other pulse types are "final" and are not good indicators of
            # us still doing something. pulse.WorkingOnIt is the only one that may
            # suggest that there is a reason to stay
            if dont_even_look_at_clock_until_done_cnt > inner_pulse.Total and inner_pulse.WorkingOnIt:
                return False
            # no "else" Falling through on other conditionals

            # This is some 10 minute mark. Hence us checking if we are still doing something
            # long-running in the last 3 minutes.
            if should_be_done_by < time.time() and not inner_pulse.WorkingOnIt:
                logger.warning(
                    f'[oozer-run][{sweep_id}][stop-reason] Stopping due to running out of time on first checkpoint'
                    f' with following pulse: {inner_pulse}'
                    f' and last score {last_processed_score}'
                )
                return True

            # This is half-hour mark. No point waiting longer than this
            # even if there are still workers there. They will die off at some point
            if really_really_kill_it_by < time.time():
                logger.warning(
                    f'[oozer-run][{sweep_id}][stop-reason] Stopping due to running out of time on last checkpoint'
                    f' with following pulse: {inner_pulse}'
                    f' and last score {last_processed_score}'
                )
                return True

        return False

    # we wait for certain number of tasks to be done, until we run out of waiting time
    pulse = sweep_tracker.get_pulse()
    _last = 0
    with Measure.counter(_measurement_name_base + 'done', tags=_measurement_tags) as cntr:

        while not its_time_to_quit(pulse):
            cntr += pulse.Total - _last
            _last = pulse.Total

            running_time = int(time.time() - start_of_run_seconds)
            logger.info(f"#{sweep_id}: ({running_time} seconds in) Waiting on {cnt} jobs with last pulse being {pulse}")
            gevent.sleep(time_slice_length)
            pulse = sweep_tracker.get_pulse()

        cntr += pulse.Total - _last

    logger.warning(
        f'[oozer-run][{sweep_id}][exiting] Exited oozer'
        f' with following pulse: {sweep_tracker.get_pulse()}'
        f' and last score {last_processed_score}'
    )
    return cnt, pulse


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

    return delay_next_sweep_start_by
