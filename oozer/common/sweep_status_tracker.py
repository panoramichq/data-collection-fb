import time
from collections import namedtuple

from typing import Union, List, Dict

import gevent

from common.connect.redis import get_redis
from common.enums.failure_bucket import FailureBucket

# attr names are same as names of attrs in FailureBucket enum
from common.measurement import Measure

Pulse = namedtuple('Pulse', list(FailureBucket.attr_name_enum_value_map.keys()) + ['Total'])

AGGREGATE_RECORD_MARKER = 'aggregate'
IN_PROGRESS_RECORD_MARKER = 'in_progress'


class SweepStatusTracker:
    def __init__(self, sweep_id: str):
        self.sweep_id = sweep_id
        self.redis = get_redis()

    def __enter__(self) -> 'SweepStatusTracker':
        return self

    def __exit__(self, *args):
        # kill outstanding tasks?
        pass

    def _gen_key(self, minute_or_marker: Union[int, str]) -> str:
        return f'{self.sweep_id}:{minute_or_marker}:{self.__class__.__name__}'

    @staticmethod
    def now_in_minutes() -> int:
        return int(time.time() / 60)

    def report_status(self, failure_bucket: int = FailureBucket.Success):
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
        self.redis.hincrby(self._gen_key(self.now_in_minutes()), failure_bucket)
        if failure_bucket < 0:
            # it's one of those temporary "i am still doing work" status types
            # like WorkingOnIt = -100
            # we don't roll those into aggregate numbers
            # as that would result in double-counting jobs
            self.redis.incrby(self._gen_key(IN_PROGRESS_RECORD_MARKER), 1)
        else:
            self.redis.hincrby(self._gen_key(AGGREGATE_RECORD_MARKER), failure_bucket)
            self.redis.incrby(self._gen_key(IN_PROGRESS_RECORD_MARKER), -1)

    def _get_aggregate_data(self) -> Dict[int, int]:
        # This mess is here just to get through the annoyance
        # of beating what used to be ints as keys AND values
        # back into ints from strings (into which Redis beats non-string
        # keys and values)
        return {int(k): int(v) for k, v in self.redis.hgetall(self._gen_key(AGGREGATE_RECORD_MARKER)).items()}

    def _get_trailing_minutes_data(self, minute: int) -> List[Dict[int, int]]:
        # This mess is here just to get through the annoyance
        # of beating what used to be ints as keys AND values
        # back into ints from strings (into which Redis beats non-string
        # keys and values)
        return [{int(k): int(v) for k, v in self.redis.hgetall(self._gen_key(minute - i)).items()} for i in range(0, 4)]

    def get_pulse(self, minute: int = None, ignore_cache: bool = False) -> Pulse:
        """
        This calculates some aggregate of status for the *most recent* jobs

        Again, this pulse is representative of some LAST FEW MINUTES of processing
        not of the entire sweep.

        Meaning of this is very particular to the use in Looper. No grand magic.
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
            FailureBucket.UserThrottling: 0,
            FailureBucket.ApplicationThrottling: 0,
            FailureBucket.AdAccountThrottling: 0,
            FailureBucket.TooLarge: 0,
            FailureBucket.WorkingOnIt: 0,
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
        for data, ratio in zip([m1, m2, m3], [0.80, 0.15, 0.05]):
            minute_total = sum(data.values())
            if minute_total:
                for k in list(result.keys()):
                    contributor = ratio * data.get(k, 0) / minute_total
                    result[k] = result[k] + contributor

        aggregate_data = self._get_aggregate_data()

        # Again, note the split:
        # - total is Done COUNT per entire sweep.
        # - rest of values are proportion of 1 (percentage as decimal)
        #   of specific outcomes in the last ~3 minutes of the run.
        #   These ratios are not representative of entire sweep so far.
        #   They are representative of "very recent" tail of sweep.
        pulse = Pulse(
            Total=sum(aggregate_data.values()),
            **{name: result.get(enum_value, 0) for name, enum_value in FailureBucket.attr_name_enum_value_map.items()},
        )

        return pulse

    def start_metrics_collector(self, interval: int):
        """Start reporting metrics to Datadog in a regular interval."""
        gevent.spawn(self._report_metrics, interval)

    def _report_metrics(self, interval: int):
        """Regularly report pulse metrics for previous minute to Datadog."""
        name_map = {
            FailureBucket.Success: 'success',
            FailureBucket.Other: 'other',
            FailureBucket.Throttling: 'throttling',
            FailureBucket.UserThrottling: 'user_throttling',
            FailureBucket.ApplicationThrottling: 'application_throttling',
            FailureBucket.AdAccountThrottling: 'adaccount_throttling',
            FailureBucket.TooLarge: 'too_large',
            FailureBucket.WorkingOnIt: 'working_on_it',
        }

        while True:
            gevent.sleep(interval)
            prev_minute = self.now_in_minutes() - 1
            pulse_values = {int(k): int(v) for k, v in self.redis.hgetall(self._gen_key(prev_minute)).items()}

            total = 0
            for bucket, name in name_map.items():
                value = pulse_values.get(bucket, 0)
                total += value
                Measure.histogram(f'{__name__}.pulse_stats', tags={'sweep_id': self.sweep_id, 'bucket': name})(value)

            if total:
                Measure.histogram(f'{__name__}.pulse_stats', tags={'sweep_id': self.sweep_id, 'bucket': 'total'})(total)

            in_progress = int(self.redis.get(self._gen_key(IN_PROGRESS_RECORD_MARKER)) or 0)
            Measure.histogram(f'{__name__}.pulse_stats', tags={'sweep_id': self.sweep_id, 'bucket': 'in_progress'})(
                in_progress
            )
