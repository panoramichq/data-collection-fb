import gevent
import logging
import math
import time

from itertools import islice
from typing import Generator, Tuple, Union, Callable

from common.celeryapp import CeleryTask
from common.id_tools import parse_id
from common.math import adapt_decay_rate_to_population, get_decay_proportion
from common.measurement import Measure
from common.timeout import timeout
from config import looper as looper_config
from config.looper import MIN_STARTING_FREQUENCY
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sorted_jobs_queue import SortedJobsQueue
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.common.sweep_status_tracker import SweepStatusTracker, Pulse
from oozer.inventory import resolve_job_scope_to_celery_task

logger = logging.getLogger(__name__)


class OozingDecayOverflow(BaseException):
    pass


def iter_tasks(sweep_id: str) -> Generator[Tuple[CeleryTask, JobScope, JobContext], None, None]:
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


def create_decay_function(num_accounts: int, num_tasks: int) -> Callable[[Union[float, int]], Union[float, int]]:
    a = max(MIN_STARTING_FREQUENCY, math.sqrt(num_accounts) + math.sqrt(num_tasks / num_accounts))
    b = -1 / (2 * math.log(num_tasks))
    cut_off = a / -b

    logger.warning(
        f'Initial settings for decay function are '
        + f'a={a}; b={b}; cut_off={cut_off}; accounts={num_accounts}; tasks={num_tasks}'
    )

    def calculate(x: Union[float, int]) -> Union[float, int]:
        if x >= cut_off:
            raise OozingDecayOverflow(f'Oozing part ran out of time (time={x}) defined by decay function - {cut_off}')

        return (a + b * x) / 2

    return calculate


def find_area_covered_so_far(fn: Callable[[float], Union[float, int]], x: float) -> int:
    """
    Computes total population ("area") of tasks we should have already processed
    by the time we are at this value of x

    zk|                      |
      |`-,_ P                |
    y |----i-,_              |
    k |----|-----------------|
      |    |       `-,_      |
      |____|___________`-,___|
     0     x             r   t

    The area is zk-P-x-0

    NOTE: This only works for cases when f(x) > 0

    :param n: Total area (total number of tasks)
    :param fn: linear equation y = F(x)
    :param x: value of x
    :return: Total population ("area") of zk-P-x-0
    """
    y = fn(x)
    # sum of y-x rectangle and zk-P-y triangle areas
    return math.ceil(y * x + (fn(0) - y) * x / 2)


class TaskOozer:
    def __init__(self, num_accounts: int, num_tasks: int, time_slice_length: int = 1):
        """
        :param num_accounts: Number of accounts to work with
        :param num_tasks: Number of tasks to release
        :param time_slice_length: in seconds. can be fractional.
        """
        self.actual_processed: int = 0
        self.time_slice_length: int = time_slice_length

        # doing this odd way of creating a method to trap decay_fn in the closure
        start_time: int = round(time.time()) - 1
        decay_fn = create_decay_function(num_accounts, num_tasks)

        def fn():
            return find_area_covered_so_far(decay_fn, (round(time.time()) - start_time) / time_slice_length)

        self.get_normative_processed: Callable[[], int] = fn

    def ooze_task(self, task: CeleryTask, job_scope: JobScope, job_context: JobContext) -> bool:
        """
        Tracks the number of calls
        """
        # *** WE ARE BLOCKING HERE ****
        # (Albeit in a concurrent way)
        # This means that calling process will be stuck waiting for us to exit,
        # without even knowing they are blocked.

        try:
            while self.actual_processed > self.get_normative_processed():
                gevent.sleep(self.time_slice_length)

            task.delay(job_scope, job_context)
            self.actual_processed += 1
            return True
        except OozingDecayOverflow as ex:
            logger.warning(str(ex))
            return False

    def __enter__(self):
        return self.ooze_task

    def __exit__(self, *args):
        # kill outstanding tasks?
        pass


@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@timeout(looper_config.RUN_TASKS_TIMEOUT)
def run_tasks(
    sweep_id: str, limit: int = None, time_slices: int = looper_config.FB_THROTTLING_WINDOW, time_slice_length: int = 1
):
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
    _measurement_name_base = __name__ + '.run_tasks.'  # <- function name. adjust if changed
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

    tasks_iter = iter_tasks(sweep_id)
    if limit:
        tasks_iter = islice(tasks_iter, 0, limit)

    def task_iter_score_gate(tasks_iter):
        for celery_task, job_scope, job_context, score in tasks_iter:
            if score < 2:  # arbitrary
                # cut the flow of tasks
                return

            # We need to see into the jobs scoring state per sweep
            Measure.counter(
                _measurement_name_base + 'job_scores',
                tags={'score': score, 'ad_account_id': job_scope.ad_account_id, **_measurement_tags},
            ).increment()

            yield celery_task, job_scope, job_context

    tasks_iter = task_iter_score_gate(tasks_iter)

    with TaskOozer(num_accounts, num_tasks, time_slice_length) as ooze_task, Measure.counter(
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
        for celery_task, job_scope, job_context in islice(tasks_iter, 0, 100):
            # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
            # It will unblock by itself when it's time to release the task
            keep_going = ooze_task(celery_task, job_scope, job_context)
            if not keep_going:
                break
            cnt += 1

        if keep_going:
            cntr += cnt

            # if there are 1st quarter tasks left in queue, burn them out
            for celery_task, job_scope, job_context in tasks_iter:
                keep_going = ooze_task(celery_task, job_scope, job_context)
                if keep_going:
                    cnt += 1

                    if cnt % _step == 0:
                        cntr += _step

                if time.time() > quarter_time or not keep_going:
                    logger.info(f"Breaking early in 1st quarter time, I am too slow {time.time()} / {quarter_time}")
                    break  # to next for-loop

        if keep_going:
            for celery_task, job_scope, job_context in tasks_iter:
                # If we are here, it's start of 2nd quarter of our total time slice
                # and we still have tasks to push out.
                # At this point we should start caring about results
                # We will look for most obvious signs of failure

                now = time.time()
                if next_pulse_review_second < now:
                    pulse = sweep_tracker.get_pulse()  # type: Pulse
                    if pulse.Total > 20:
                        if pulse.Success < 0.10:  # percent
                            # failures across the board
                            # return cnt, pulse
                            logger.info(
                                "Breaking early in 2nd quarter time, due to too many failures of any kind "
                                + "(more than 10 percent)"
                            )
                            break
                        if pulse.Throttling > 0.40:  # percent
                            # time to give it a rest
                            # return cnt, pulse
                            logger.info("Breaking early in 2nd quarter time, due to throttling (more than 40 percent)")
                            break
                    next_pulse_review_second = now + _pulse_refresh_interval

                # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
                # It will unblock by itself when it's time to release the task
                keep_going = ooze_task(celery_task, job_scope, job_context)
                if keep_going:
                    cnt += 1

                    if cnt % _step == 0:
                        cntr += _step

                    if now > half_time:
                        logger.info(f"Breaking early in 2nd quarter time, I am too slow {now}/{half_time}")
                        break
                else:
                    break

        # In second half of the loop, if we still have tasks to release
        # we might need to cut the tail of the task queue if we now realize we will not
        # burn through it all.
        # For that we need this:
        pulse = sweep_tracker.get_pulse()  # type: Pulse
        half_time_done_cnt = pulse.Total
        # Note, that we may be here in the first second of the sweep loop if we have
        # some very few seed tasks. So, this is NOT guaranteed to be exactly the middle
        # of the sweep loop as far as time is concerned. This could be "the end" of the loop
        # as far as tasks oozing is concerned and "the very beginning" of the loop in terms of time.
        # this value is "half_time" only if the next for loop has any items to process.
        # So, don't do any "half" logic here. Do it inside this next for loop.
        cut_off_at_cnt = num_tasks

        if keep_going:
            for celery_task, job_scope, job_context in tasks_iter:
                # If we are here, exactly start of 2nd half of our total time slice
                # and we still have tasks to push out.

                # since we need only one swing through the loop - just to ensure
                # there is something there,
                # could have done tasks_iter.__next__() wrapped in try-catch, but
                # did not want to break the pattern.

                cut_off_at_cnt = min(cut_off_at_cnt, half_time_done_cnt * 2)

                keep_going = ooze_task(celery_task, job_scope, job_context)
                if keep_going:
                    cnt += 1

                    if cnt % _step == 0:
                        cntr += _step

                break

        if keep_going:
            for celery_task, job_scope, job_context in tasks_iter:
                # If we are here, we are a little bit into 2nd half of our total time slice
                # and we still have tasks to push out.

                # At this point we should start caring about what we queue up,
                # because by about half-time we need to know if we cut the cycle or not
                # Here we are building linear equation derived from observing rate and speed of
                # completion of the tasks we already pushed out and trying to predict
                # what happens to jobs we would push out from this point on.

                now = time.time()

                if next_pulse_review_second < now:
                    pulse = sweep_tracker.get_pulse()  # type: Pulse
                    if pulse.Total > 20:
                        if pulse.Success < 0.20:  # percent
                            # failures across the board
                            # return cnt, pulse
                            logger.info(
                                "Breaking 2nd halif time, due to too many failures of any kind (more than 20 percent)"
                            )
                            break
                        if pulse.Throttling > 0.40:  # percent
                            # time to give it a rest
                            # return cnt, pulse
                            logger.info("Breaking early in 2nd quarter time, due to throttling (more than 40 percent)")
                            break
                    next_pulse_review_second = time.time() + _pulse_refresh_interval

                # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
                # It will unblock by itself when it's time to release the task
                keep_going = ooze_task(celery_task, job_scope, job_context)
                if keep_going:
                    cnt += 1

                    if cnt % _step == 0:
                        cntr += _step

                    if cnt > cut_off_at_cnt:
                        if cnt < num_tasks:
                            logger.info(f"#{sweep_id}: Queueing cut at {cnt} jobs of total {num_tasks}")
                        break
                else:
                    break

                logger.info(f"#{sweep_id}: Queued up all jobs {num_tasks}")

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

    def its_time_to_quit(pulse: Pulse) -> bool:
        # must have anything at all done
        # otherwise logic below makes no sense
        # This stops us from quitting in very beginning of the loop
        # global dont_even_look_at_clock_until_done_cnt, really_really_kill_it_by, should_be_done_cnt

        if should_be_done_cnt <= pulse.Total:
            # Yey! all done!
            return True

        if pulse.Total:
            # all other pulse types are "final" and are not good indicators of
            # us still doing something. pulse.WorkingOnIt is the only one that may
            # suggest that there is a reason to stay
            if dont_even_look_at_clock_until_done_cnt > pulse.Total and pulse.WorkingOnIt:
                return False
            # no "else" Falling through on other conditionals

            # This is some 10 minute mark. Hence us checking if we are still doing something
            # long-running in the last 3 minutes.
            if should_be_done_by < time.time() and not pulse.WorkingOnIt:
                return True

            # This is half-hour mark. No point waiting longer than this
            # even if there are still workers there. They will die off at some point
            if really_really_kill_it_by < time.time():
                return True

        return False

    # we wait for certain number of tasks to be done, until we run out of waiting time
    pulse = sweep_tracker.get_pulse()  # type: Pulse
    _last = 0
    with Measure.counter(_measurement_name_base + 'done', tags=_measurement_tags) as cntr:

        while not its_time_to_quit(pulse):
            cntr += pulse.Total - _last
            _last = pulse.Total

            running_time = int(time.time() - start_of_run_seconds)
            logger.info(f"#{sweep_id}: ({running_time} seconds in) Waiting on {cnt} jobs with last pulse being {pulse}")
            gevent.sleep(time_slice_length)
            pulse = sweep_tracker.get_pulse()  # type: Pulse

        cntr += pulse.Total - _last

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
