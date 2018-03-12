import functools
from contextlib import ContextDecorator
from time import time

from datadog.dogstatsd import DogStatsd

from config import build, measurement


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

        Useful say for timers, but other uses are also available

        @Measure.timer('mymetric')
        def my_function(argument1, measure):
            measure(1234)


    """
    def __init__(
            self,
            # Auto bound methods
            measure_function, prefix, default_value,
            # Actual invocation related methods
            metric, tags=None, sample_rate=1
    ):
        self._statsd_func = measure_function
        self._default_value = default_value

        # Add metric prefix
        self._metric = '.'.join([prefix, metric])

        # We may have empty tags
        self._tags = tags or {}

        self._sample_rate = sample_rate

    def _measure(self, value):
        """
        Initiate the actual measurement

        :param any value: Any value to be sent along to statsd
        """
        # Apply default value if needed
        value = value or self._default_value

        self._statsd_func(
            self._metric,
            value,
            _dict_as_statsd_tags(self._tags),
            self._sample_rate
        )

    def __call__(self, argument):
        """
        Either we are calling this as the measurement function, and therefore
        we want to actually send measure data, or, this is a decorator and
        therefore, we call the super of ContextDecorator, to allow to be used
        as such.

        Note that we are injecting the attribute `measurement`, so that the
        function has our metric available

        Inject the measurement wrapper to the function.
        """
        if callable(argument):
            return super().__call__(
                functools.partial(argument, measurement=self)
            )

        self._measure(argument)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class AutotimingMeasuringPrimitive(MeasuringPrimitive):
    """
    Provides automatic timing either for a context or as a decorator.

    You cannot call this measure directly (as that would make no sense really)
    """

    _start = None
    """
    The time that elapsed during the runtime of this measurement
    """

    _stop = None
    """
    Stop time, if available
    """

    @property
    def elapsed(self):
        # Either we're still in flight, or this timer has been stopped
        stop_time = self._stop or time()

        # Normalize to millis
        elapsed = stop_time - self._start
        elapsed = int(round(1000 * elapsed))
        return elapsed

    def __call__(self, argument):
        """
        We are forbidding direct measurements here
        """

        if not callable(argument):
            msg = "You cannot use autotiming measurement directly, use either" \
                  " with a context manager or a decorator"
            raise RuntimeError(msg)

        return super().__call__(argument)

    def __enter__(self):
        """
        Kick off the timer
        :return:
        """
        self._start = time()
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the timer and measure value
        """
        self._stop = time()
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

    @property
    def total_value(self):
        return self._total_value

    def increment(self, by_how_much):
        """
        Just send along the value
        """
        self._total_value += by_how_much
        self._measure(by_how_much)

    def decrement(self, by_how_much):
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

    def __call__(self, argument):
        """
        We are forbidding direct measurements here, as you need to use either
        direct methods or operators
        """

        if not callable(argument):
            msg = "You cannot use counter measurement directly, use either" \
                  " with a context manager or a decorator"
            raise RuntimeError(msg)

        return super().__call__(argument)


class MeasureWrapper:
    """
    Wraps connection to the statsd server and creates wrapped measuring methods
    on top of the class.

    Provided you create an instance measure = MeasureWrapper(*args), you can
    then use it like this:

    measure.increment('metric')(value)

    or with context managers:

    with measure.increment('metric') as m:
        m(value)

    or with function decorators

    @measure.increment('metric')
    def my_method(blah, measuring_context):
        measuring_context(value)

    """

    _statsd = None

    # Statsd primitives
    increment = None   # type: MeasuringPrimitive
    decrement = None   # type: MeasuringPrimitive
    gauge = None       # type: MeasuringPrimitive
    timing = None      # type: MeasuringPrimitive
    set = None         # type: MeasuringPrimitive

    # Datadogs primitives
    # TODO: include them too? Will be easy

    # Our own little bit more interesting measuring primitives
    counter = None     # type: MeasuringPrimitive
    autotiming = None  # type: MeasuringPrimitive

    def __init__(
        self, enabled, statsd_host, statsd_port, prefix=None, default_tags=None
    ):
        """
        This is a wrapper that does primarily this:

        - setup connection to statsd server
        - wrap measuring methods such that they can be used as various things
            (context managers, decorators)
        -


        :param bool enabled: Is measurement enabled
        :param string statsd_host: Host of the statsd server
        :param int statsd_port: Port of the statsd server
        :param string prefix: Default prefix to add to all metrics
        :param dict|None default_tags: Default tags to add to all metrics
        """
        # Setup stats connection
        self._statsd = DogStatsd(
            host=statsd_host, port=statsd_port,
            constant_tags=_dict_as_statsd_tags(default_tags)
        )

        # TODO: add metric specific prefixes (low pri, probably)

        # Add measurement methods
        self.increment = self._wrap_measurement_method(
            enabled, self._statsd.increment, default_value=1, prefix=prefix
        )
        self.decrement = self._wrap_measurement_method(
            enabled, self._statsd.decrement, default_value=1, prefix=prefix
        )
        self.gauge = self._wrap_measurement_method(
            enabled, self._statsd.gauge, prefix=prefix
        )

        self.timing = self._wrap_measurement_method(
            enabled, self._statsd.timing, prefix=prefix
        )
        self.set = self._wrap_measurement_method(
            enabled, self._statsd.set, prefix=prefix
        )

        # Our own augmented measurement primitives
        self.autotiming = self._wrap_measurement_method(
            enabled, self._statsd.timing, prefix=prefix,
            wrapper=AutotimingMeasuringPrimitive
        )

        self.counter = self._wrap_measurement_method(
            enabled, self._statsd.increment, prefix=prefix,
            wrapper=CounterMeasuringPrimitive
        )

    def _wrap_measurement_method(
        self, enabled, func, prefix, default_value=None, wrapper=None
    ):
        """
        We need to wrap the singular measurement function with our
        MeasuringPrimitive (or a subclass thereof) class, so that we can support
        various interfaces on top of it.

        If the measurements are disabled, we just replace the statsd function do
        nothing.

        :param bool enabled: Whether the measurement should actually send it's
            values
        :param function func: The function to be wrapped
        :param string prefix: Common metric prefix
        :param any default_value: Default value for the metric
        :param MeasuringPrimitive wrapper: The wrapper class to use
        :return function: The partial to be called on the MeasuringPrimitive
            constructor
        """
        if not enabled:
            func = lambda *args, **kwargs: None

        return functools.partial(
            wrapper or MeasuringPrimitive, func, prefix, default_value
        )


# Instance of the measuring tools injected with configuration options
Measure = MeasureWrapper(
    enabled=measurement.ENABLED,
    statsd_host=measurement.STATSD_SERVER,
    statsd_port=measurement.STATSD_PORT,
    prefix=measurement.METRIC_PREFIX,
    default_tags={
        'build_id': build.BUILD_ID,
        'commit_id': build.COMMIT_ID,
    }
)
