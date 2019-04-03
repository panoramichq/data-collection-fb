import functools
from contextlib import ContextDecorator
from time import time
import logging

from datadog.dogstatsd import DogStatsd

import config.measurement
import config.application
import config.build

logger = logging.getLogger(__name__)


def _dict_as_statsd_tags(tags):
    """
    Reformat tags to somethign statsd understands

    :param dict tags: Tags in dictionary form
    :return list:
    """
    return [f'{tag}:{tag_value}' for tag, tag_value in tags.items()]


class MeasuringPrimitive(ContextDecorator):
    """
    A wrapper for measuring functions that adds some application wide stuff
    and capabilities.

    In general:
        - common prefix for all metrics,
        - default value for given metric measurement,
        - different convenience ways to use the measuring function

    In general, the signature of calls to given singular measurement method is:

    measurement_function(value=10, tags={'my_tag': 'tag_value'}, sample_rate=0.5)

    Also, the wrapper gives you multiple ways to use particular measurement:

    - directly:
        For simple measurement one-off calls

        you call Measure.increment('mymetric', tags={'tag': 'val'})(value)
            and set the value

    - context manager

        Useful when you know you will be calling the metric function more than
        once

        with Measure.increment('mymetric') as ctx:
            ctx()

            ... blah

            ctx(2)

    - function decorator

        Useful say for timers, but other uses are also available. If you want,
        the Measure reference, use bind=True in the decorator

        @Measure.timer('mymetric', bind=True)
        def my_function(argument1, Measure):
            Measure(1234)


    """

    def __init__(
        self,
        # Auto bound methods
        measure_function, prefix, default_value,
        # Actual invocation related methods
        metric, tags=None, sample_rate=1,
        # Binding options and so on
        bind=None, function_name_as_metric=False,
        extract_tags_from_arguments=None,
    ):
        """

        :param measure_function:
        :param prefix:
        :param default_value:
        :param metric:
        :param tags:
        :param sample_rate:
        :param bind: String indicating the name of kwarg to inject into wrapped function
        :param function_name_as_metric:
        :param extract_tags_from_arguments: extracts tag from wrappee's arguments
        """
        self._statsd_func = measure_function
        self._default_value = default_value

        # Add metric prefix
        self._metric = '.'.join(filter(None, [prefix, metric]))

        # We may have empty tags
        self._tags = tags or {}

        self._sample_rate = sample_rate

        # Decorators only: If bind is true, it will bind this measuring
        # primitive as `Measure` function argument
        self._bind = bind

        # Decorators only: If function_name_as_metric is true, it will append
        # the function name to the metric name, joined on . (dot) character
        self._function_name_as_metric = function_name_as_metric
        self._extract_tags_from_arguments = extract_tags_from_arguments
        self._extracted_tags = {}

    def _wrap_callable(self, func, hook=None):
        """
        Wraps a function you would supply when overriding `__call__` somewhere
        down the chain for callables. Useful for not messing up autonaming of
        metrics and having always a "well-behaved' decorators.

        Extending this to pre and post hooks is also pretty simple obviously, if
        we need to do that

        :param callable func: The callable we are wrapping
        :param callable hook: If supplied, will be called as part of the wrapper
            The optionality is given the fact it is encouraged to use this
            wrapper everywhere for clarity
        :return:
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if hook:
                hook()
            if self._extract_tags_from_arguments:
                self._extracted_tags = self._extract_tags_from_arguments(*args, **kwargs)
            return func(*args, **kwargs)

        return wrapper

    def _measure(self, value):
        """
        Initiate the actual measurement

        :param any value: Any value to be sent along to statsd
        """
        # Apply default value if needed
        value = value or self._default_value

        logger.debug(f"Submitting metric: {self._metric}")
        final_tags = {
            **self._extracted_tags,
            **self._tags,
        }
        self._statsd_func(self._metric, value, _dict_as_statsd_tags(final_tags), self._sample_rate)

    def __call__(self, argument):
        """
        Either we are calling this as the measurement function, and therefore
        we want to actually send Measure data, or, this is a decorator and
        therefore, we call the super of ContextDecorator, to allow to be used
        as such.

        Note that we are injecting the attribute `measurement`, so that the
        function has our metric available

        Inject the measurement wrapper to the function.
        """
        if callable(argument):
            # This means we are using this as decorator

            # Optionally bind the Measure to the function
            if self._bind:
                argument = functools.partial(
                    argument,
                    **{
                        'measure' if self._bind is True else self._bind : self
                    }
                )

            # Optionally append callable name as metric name
            if self._function_name_as_metric:
                self._metric = '.'.join([self._metric, argument.__name__])

            return super().__call__(self._wrap_callable(argument))

        self._measure(argument)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class TimerMeasuringPrimitive(MeasuringPrimitive):
    """
    Provides automatic timing either for a context or as a decorator.

    You cannot call this Measure directly (as that would make no sense really)
    """

    _start = None
    """
    The time that elapsed during the runtime of this measurement
    """

    _stop = None
    """
    Stop time, if available
    """

    @staticmethod
    def _get_now_in_seconds():
        """
        Factored out for ease of testing
        :return: Now in seconds (with decimals expressing sub-second time)
        :rtype: float
        """
        return time()

    @property
    def elapsed(self):
        # Either we're still in flight, or this timer has been stopped
        stop_time = self._stop or self._get_now_in_seconds()

        # Normalize to millis
        elapsed = stop_time - self._start
        elapsed = int(round(1000 * elapsed))
        return elapsed

    def __call__(self, argument):
        """
        We are forbidding direct measurements here

        @:return MeasuringPrimitive
        """

        if not callable(argument):
            msg = "You cannot use timer measurement directly, use either" \
                  " with a context manager or a decorator"
            raise RuntimeError(msg)

        return super().__call__(self._wrap_callable(argument))

    def __enter__(self):
        """
        Kick off the timer
        :return:
        """
        self._start = self._get_now_in_seconds()
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the timer and Measure value
        """
        self._stop = self._get_now_in_seconds()
        self._measure(self.elapsed)
        super().__exit__(exc_type, exc_val, exc_tb)


class CounterMeasuringPrimitive(MeasuringPrimitive):
    """
    Provides a different way how to use increment, decrement, as a convenience
    method.

    Useful specifically for decorators / context managers, where a value might
    fluctuate over the course of the execution.

    Convenience += and -= operators are also available
    """

    _total_value = 0
    """
    A running tally on the value. The value is here just to have a finger on it
    if needed, but this is not what is sent to statsd really, and *may* span
    over several statsd flushes, so this is value will *not* necessarily 
    correspond to what you will see in measurement charts  
    """

    _count_once = False
    """
    If this is set to true, the counter will ensure that the counter will be
    always set to 1 at the beginning (you may increment it further if you wish)
    """

    def __init__(self, *args, count_once=False, **kwargs):
        super().__init__(*args, **kwargs)

        # If set to true, we will ensure that this metric will be sent along
        # *at minimum* with count of one (eg when entered or called through a
        # deco, `increment` is called
        self._count_once = count_once

    @property
    def total_value(self):
        return self._total_value

    def increment(self, by_how_much=1):
        """
        Just send along the value
        """
        self._total_value += by_how_much
        self._measure(by_how_much)

    def decrement(self, by_how_much=1):
        """
        Inverts the value to negative
        """
        self._total_value -= by_how_much
        self._measure(-by_how_much)

    def __iadd__(self, other):
        """
        For use like measurement+=10
        """
        self.increment(other)
        return self

    def __isub__(self, other):
        """
        For use like measurement-=10
        """
        self.decrement(other)
        return self

    def _wrapper_hook(self):
        """
        Automatically increment once, regardless of what the situation is
        """
        if self._count_once:
            self.increment(1)

    def __call__(self, argument):
        """
        We are forbidding direct measurements here, as you need to use either
        direct methods or operators
        """

        if not callable(argument):
            msg = "You cannot use counter measurement directly, use either" \
                  " with a context manager or a decorator"
            raise RuntimeError(msg)

        return super().__call__(self._wrap_callable(argument, self._wrapper_hook))


# TODO: Think about whether we actually want something like this...
# class CompoundMeasuringPrimitives:
#     """
#     This simplifies the usage of measuring primitives that you would often use
#     together, so you do not have to decorate them everywhere.
#
#     This is just a container that creates combined decorators
#     """
#
#     @classmethod
#     def _create_compound_measuring_primitive(cls, *args):
#         """
#         Wraps a list of measuring primitives together
#         :return MeasuringPrimitive
#         """
#         if len(args) < 2:
#             raise RuntimeError(
#                 "You need to supply at leas 2 measuring primitives"
#             )
#
#         def wrapper(func):
#             # Boot with the initial function
#             compound = func
#
#             for measure_function in args:
#                 compound = measure_function(compound)
#
#             return compound
#
#         return wrapper
#
#     @classmethod
#     def function_time_and_count(cls, metric):
#         return cls._create_compound_measuring_primitive(
#             Measure.counter(metric, function_name_as_metric=True, count_once=True),
#             Measure.timer(metric, function_name_as_metric=True)
#         )


class MeasureWrapper:
    """
    Wraps connection to the statsd server and creates wrapped measuring methods
    on top of the class.

    Provided you create an instance Measure = MeasureWrapper(*args), you can
    then use it like this:

    Measure.increment('metric')(value)

    or with context managers:

    with Measure.increment('metric') as m:
        m(value)

    or with function decorators

    @Measure.increment('metric')
    def my_method(blah, measuring_context):
        measuring_context(value)
    """

    _statsd = None

    # Statsd primitives
    decrement = None  # type: MeasuringPrimitive
    gauge = None  # type: MeasuringPrimitive
    histogram = None  # type: MeasuringPrimitive
    increment = None  # type: MeasuringPrimitive
    set = None  # type: MeasuringPrimitive
    timing = None  # type: MeasuringPrimitive

    # Datadogs primitives
    # TODO: include them too? Will be easy

    # Our own little bit more interesting measuring primitives
    counter = None     # type: CounterMeasuringPrimitive
    timer = None  # type: TimerMeasuringPrimitive

    def __init__(
        self, host='localhost', port=8125, prefix=None, default_tags=None
    ):
        """
        This is a wrapper that does primarily this:

        - setup connection to statsd server
        - wrap measuring methods such that they can be used as various things
            (context managers, decorators)
        -


        :param string host: Host of the statsd server
        :param int port: Port of the statsd server
        :param string prefix: Default prefix to add to all metrics
        :param dict|None default_tags: Default tags to add to all metrics
        """
        # Setup stats connection
        self._statsd = DogStatsd(
            host=host,
            port=port,
            constant_tags=_dict_as_statsd_tags(default_tags)
        )

        # Add measurement methods
        self.increment = self._wrap_measurement_method(
            self._statsd.increment, default_value=1,
            prefix=self._join_with_prefix(config.measurement.PREFIX_COUNTER, prefix)
        )
        self.decrement = self._wrap_measurement_method(
            self._statsd.decrement, default_value=1,
            prefix=self._join_with_prefix(config.measurement.PREFIX_COUNTER, prefix)
        )
        self.gauge = self._wrap_measurement_method(
            self._statsd.gauge,
            prefix=self._join_with_prefix(config.measurement.PREFIX_GAUGE, prefix)
        )

        self.histogram = self._wrap_measurement_method(
            self._statsd.histogram,
            prefix=self._join_with_prefix(config.measurement.PREFIX_HISTOGRAM, prefix)
        )

        self.timing = self._wrap_measurement_method(
            self._statsd.timing,
            prefix=self._join_with_prefix(config.measurement.PREFIX_TIMING, prefix)
        )
        self.set = self._wrap_measurement_method(
            self._statsd.set,
            prefix=self._join_with_prefix(config.measurement.PREFIX_SET, prefix)
        )

        # Our own augmented measurement primitives
        self.timer = self._wrap_measurement_method(
            self._statsd.timing,
            prefix=self._join_with_prefix(config.measurement.PREFIX_TIMING, prefix),
            wrapper=TimerMeasuringPrimitive
        )

        self.counter = self._wrap_measurement_method(
            self._statsd.increment,
            prefix=self._join_with_prefix(config.measurement.PREFIX_COUNTER, prefix),
            wrapper=CounterMeasuringPrimitive
        )

    def _wrap_measurement_method(
        self, func, prefix, default_value=None, wrapper=None
    ):
        """
        We need to wrap the singular measurement function with our
        MeasuringPrimitive (or a subclass thereof) class, so that we can support
        various interfaces on top of it.

        If the measurements are disabled, we just replace the statsd function do
        nothing.

        :param function func: The function to be wrapped
        :param string prefix: Common metric prefix
        :param any default_value: Default value for the metric
        :param MeasuringPrimitive wrapper: The wrapper class to use
        :return function: The partial to be called on the MeasuringPrimitive
            constructor
        """
        return functools.partial(
            wrapper or MeasuringPrimitive, func, prefix, default_value
        )

    def _join_with_prefix(self, value_prefix, global_prefix):
        """
        Joins prefixes together, useful for combining global and metric type
        specific prefix
        """
        return '.'.join(filter(None, [global_prefix, value_prefix]))


# Instance of the measuring tools injected with configuration options
Measure = MeasureWrapper(
    host=config.measurement.STATSD_SERVER,
    port=config.measurement.STATSD_PORT,
    # https://help.datadoghq.com/hc/en-us/articles/203764705-What-are-valid-metric-names-
    prefix=config.application.NAME.replace('-', '_'),  #
    default_tags={
        'application': config.application.NAME,
        'build_id': config.build.BUILD_ID,
        'commit_id': config.build.COMMIT_ID,
        'environment': config.application.ENVIRONMENT,
    }
)
