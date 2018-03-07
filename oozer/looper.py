import gevent
import logging
import math
import time

from collections import namedtuple, OrderedDict
from itertools import islice
from typing import Generator, Tuple, List

from common.connect.redis import get_redis
from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from common.id_tools import parse_id
from config import looper as looper_config
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.sorted_jobs_queue import SortedJobsQueue


logger = logging.getLogger(__name__)


def get_tasks_map():
    # inside of function call to avoid circular import errors

    from oozer.entities.tasks import collect_entities_per_adaccount_task, extract_adaccounts_task
    from oozer.metrics.tasks import collect_insights_task

    # handlers are often the same for each type because they
    # look at JobScope to figure out the particular data collection mode
    return {
        ReportType.console: {
          Entity.AdAccount: extract_adaccounts_task,
        },
        ReportType.entities: {
            Entity.AdAccount: None, # FIXME: Fetch ad accounts from facebook - regular pipeline
            Entity.Campaign: collect_entities_per_adaccount_task,
            Entity.AdSet: collect_entities_per_adaccount_task,
            Entity.Ad: collect_entities_per_adaccount_task,
        },
        ReportType.lifetime: {
            Entity.Campaign: collect_insights_task,
            Entity.AdSet: collect_insights_task,
            Entity.Ad: collect_insights_task,
        },
        ReportType.day_age_gender: {
            Entity.Ad: collect_insights_task
        },
        ReportType.day_dma: {
            Entity.Ad: collect_insights_task
        },
        ReportType.day_hour: {
            Entity.Ad: collect_insights_task
        }
    }


def iter_tasks(sweep_id):
    """
    Persist prioritized jobs and pass-through context objects for inspection

    :param str sweep_id:
    :rtype: Generator[Tuple[CeleryTask, JobScope, JobContext]]
    """
    from config.facebook import TOKEN

    tasks_inventory = get_tasks_map()

    with SortedJobsQueue(sweep_id).TasksReader() as jobs_iter:
        for job_id in jobs_iter:

            parts = parse_id(job_id)  # type: dict

            report_type = parts['report_type']
            entity_type = parts['entity_type'] or parts['report_variant']

            celery_task = tasks_inventory.get(report_type, {}).get(entity_type)

            if not celery_task:
                logger.warning(f"#{sweep_id}: Could not match job_id {job_id} to a worker.")
            else:
                job_scope = JobScope(
                    parts,
                    sweep_id=sweep_id,
                    tokens=[TOKEN]
                )

                # TODO: Add job context, at minimum entity hash data. TBD how to get
                # this, could be Dynamo directly, or prepared by the sweep builder
                # and sent along
                job_context = JobContext()

                yield celery_task, job_scope, job_context


def create_decay_function(n, t, z=looper_config.DECAY_FN_START_MULTIPLIER):
    """
    A function that crates a linear decay function y = F(x), where a *smooth* rationing
    (k per time slice) of population (n) of units of work into discrete time slices (t)
    is converted into decay-based allocation with larger allocations of work units per slice
    in early time slices and tapering off of work unit allocation in later time slices

    Note that at z=2 there are no remaining empty time slices at the end. With z higher than 2
    you are building a comfy padding of empty time slices at the end. With z < 2 you overshoot
    t and will not have enough slices at the end to burn off entire population of tasks.
    In order to prevent silly values of z, see assert further below

    Used to allow aggressive-from-start oozing out of tasks in the beginning of
    processing period. Also allows for a gap of time (between r and t) where
    long-trailing tasks can finish and API throttling threshold to recover.

    zk|                      |
      |`-,_                  |
    y |----i-,_              |
    k |----|-----------------|
      |    |       `-,_      |
      |____|___________`-,___|
     0     x             r   t

    Homework (please check my homework and poke me in the eye. DDotsenko):

    The task was to derive computationally-efficient decay function (exponential would be cool,
    but too much CPU for little actual gain, so settled on linear) for flushing out tasks.

    What's known in the beginning:
    - Total number of tasks - n
    - total number of periods we'd like to push the tasks over - t
      (say, we want to push out tasks over 10 minutes, and we want to do it
       every second, so 10*60=600 total periods)

    The approach taken is to jack up by multiplier z the original number of pushed out tasks
    in the very fist time slice compared to - k = n / t - what would have been pushed out if all the tasks
    were evenly allocated over all time slices.

    This becomes a simple "find slope of hypotenuse (zk-r)" problem, where we know only two things about
    that triangle:
    - rise is k*z (kz for short)
    - area of the triangle (must be same as area of k-t rectangle) - n - the total population

    To find out r let's express the area of that triangle as half-area of a rectangle zk-r
        n = zkr / 2

    From which we derive r:
        r = 2n / zk

    Thus, slope of the hypotenuse can be computed as:
        y = zk - zk/r * x

    Which reduces to:
        y = zk - (zk^2 / 2n) * x

    :param n: Total number of jobs to process
    :param t: Total number of time periods we are expecting to have
    :param x: The
    :return: A function that computes number of tasks to release
        in a given time slice given that time slice's index (from 0 to t)
    """

    assert z >= 2

    k = n / t
    zk = z * k

    a = zk
    b = -1 * zk * zk / (2 * n)

    return lambda x: math.ceil(a + b * x)


def find_area_covered_so_far(fn, x):
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

    :param n: Total area (total number of tasks)
    :param fn: linear equation y = F(x)
    :param x: value of x
    :return: Total population ("area") of zk-P-x-0
    """
    y = fn(x)
    # sum of y-x rectangle and zk-P-y triangle areas
    return math.ceil(y * x + (fn(0) - y) * x / 2)


class TaskOozer():

    def __init__(self, n, t, time_slice_length=1):
        """
        :param n: Number of tasks to release
        :param t: Time slices to release the tasks over
        :param time_slice_length: in seconds. can be fractional.
        """
        self.actual_processed = 0
        self.time_slice_length = time_slice_length

        # doing this odd way of creating a method to trap decay_fn in the closure
        start_time = round(time.time()) - 1
        decay_fn = create_decay_function(n, t)
        fn = lambda: find_area_covered_so_far(
            decay_fn,
            (round(time.time()) - start_time ) / time_slice_length
        )

        self.get_normative_processed = fn

    def ooze_task(self, task, job_scope, job_context):
        """
        Tracks the number of calls
        :param task:
        :param job_scope:
        :param job_context:
        :return:
        """

        ### WE ARE BLOCKING HERE ####
        # (Albeit in a concurrent way)
        # This means that calling process will be stuck waiting for us to exit,
        # without even knowing they are blocked.
        while self.actual_processed > self.get_normative_processed():
            gevent.sleep(self.time_slice_length)

        task.delay(job_scope, job_context)
        self.actual_processed += 1

    def __enter__(self):
        return self.ooze_task

    def __exit__(self, *args):
        # kill outstanding tasks?
        pass


# attr names are same as names of attrs in FailureBucket enum
Pulse = namedtuple(
    'Pulse',
    list(FailureBucket.attr_name_enum_value_map.keys()) + ['Total']
)


class SweepStatusTracker():

    def __init__(self, sweep_id):
        self.sweep_id = sweep_id

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # kill outstanding tasks?
        pass

    def _gen_key(self, minute):
        return f'{self.sweep_id}:{minute}:{self.__class__.__name__}'

    @staticmethod
    def now_in_minutes():
        return int(time.time()/60)

    def report_status(self, failure_bucket=None):
        """
        Every effective job calls this to indicate done-ness and severity of done-ness

        The action taken is very specific to SweepStatusTracker and its use by SweepLooper
        Whatever happens here, has no greater meaning, except to help Looper exit early.

        :param failure_bucket: A value from FailureBucket enum.
        """

        # status data is stored in '{self.sweep_id}:{minute}' keys which values are
        # hash objects. Inside the hash, the keys are values of FailureBucket enum
        # (except stringified)
        # Thus, within given minute, various tasks' status reports will fall into same
        # outter key, inside of which value for each of inner keys will be growing,
        # until we fall onto next minute, when we start fresh.

        key = self._gen_key(self.now_in_minutes())
        get_redis().hincrby(key, failure_bucket or FailureBucket.Success)

    def _get_trailing_minutes_data(self, minute):
        # This mess is here just to get through the annoyance
        # of beating what used to be ints as keys AND values
        # back into ints from strings (into which Redis beats non-string
        # keys and values)
        rr = get_redis()
        return [
            {
                int(k): int(v)
                for k, v in rr.hgetall(
                self._gen_key(minute-i)
            ).items()
            }
            for i in range(0, 4)
        ]

    def get_pulse(self, minute=None, ignore_cache=False):
        """
        This calculates some aggregate of status for the most recent jobs

        Meaning of this is very particular to the use in Looper. No grand magic.

        :return:
        """

        minute = minute or self.now_in_minutes()

        m0, m1, m2, m3 = self._get_trailing_minutes_data(minute)

        # merge data from minute zero and minute one
        # because minute zero might have only started
        # and making proportional success estimates based
        # just on it is too early
        for k, v in m0.items():
            m1[k] = m1.get(k, 0) + v

        result = {
            FailureBucket.Success: 0,
            FailureBucket.Other: 0,
            FailureBucket.Throttling: 0,
            FailureBucket.TooLarge: 0
        }

        # Now the proportion of successes, failure
        # is calculated per each minute. Then those
        # proportions are weighted by ratio related to recency
        # of that time slice. Then all of proportions are
        # rolled up by type.
        # Note, that what you get as a result in each failure
        # bucket category is NOT a true proportion to total population
        # but a quasi-score that is heavily proportion-derived.
        # The goal here is to accentuate most recent
        # success-vs-failure proportions, with padding of more
        # distant proportions.
        # Since these are voodoo numbers, feel free to rejiggle this formula,
        # but must adapt uses of them in looper below.
        total_total = 0
        for data, ratio in zip([m1, m2, m3], [80, 15, 5]):
            total = sum(data.values())
            if total:
                total_total += total
                for k in list(result.keys()):
                    result[k] = result[k] + ratio * data.get(k, 0) / total

        pulse = Pulse(
            Total=total_total,
            **{
                name: int(result.get(enum_value, 0))
                for name, enum_value in FailureBucket.attr_name_enum_value_map.items()
            }
        )

        return pulse


def run_tasks(sweep_id, limit=None, time_slices=looper_config.FB_THROTTLING_WINDOW, time_slice_length=1):
    """
    Oozes tasks gradually into Celery workers queue, accounting for total number of tasks
    and the window of time over which we want them to be processed.

    :param sweep_id:
    :param limit: Max number of tasks to push out.
        Can also be a shortcut from calling code indicating total size of population,
        allowing us to avoid checking the size of collection on disk - time savings.
    :param time_slices: Number of one-second periods to spread the tasks over
    :param time_slice_length: in seconds. can be fractional
    :return:
    """

    n = limit or SortedJobsQueue(sweep_id).get_queue_length()
    if n < time_slices:
        # you will have very few tasks released per period. Annoying
        # let's model oozer such that population is at least 10 tasks per time slice
        # If we burn through tasks earlier as a result of that assumption - fine.
        n = time_slices * 10

    max_running_time_seconds = time_slices * time_slice_length
    be_done_by = time.time() + max_running_time_seconds

    cnt = 0
    _pulse_refresh_interval = 5  # seconds
    sweep_tracker = SweepStatusTracker(sweep_id)

    tasks_iter = iter_tasks(sweep_id)
    if limit:
        tasks_iter = islice(tasks_iter, 0, limit)

    with TaskOozer(n, time_slices, time_slice_length) as ooze_task:
        next_pulse_review_second = time.time() - 1  # set in past to force refresh
        for celery_task, job_scope, job_context in tasks_iter:

            if next_pulse_review_second < time.time():
                pulse = sweep_tracker.get_pulse()  # type: Pulse
                if pulse.Total > 20:
                    if pulse.Success < 20:
                        # failures across the board
                        return cnt
                    if pulse.Throttling > 40:
                        # time to give it a rest
                        return cnt
                next_pulse_review_second = time.time() + _pulse_refresh_interval

            # FYI: ooze_task blocks if we pushed too many tasks in the allotted time
            # It will unblock by itself when it's time to release the task
            ooze_task(celery_task, job_scope, job_context)
            cnt += 1

    return cnt
