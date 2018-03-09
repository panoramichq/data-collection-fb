import functools
from contextlib import ContextDecorator

from datadog import DogStatsd

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
    # TODO: This one for automatic timing

    def __call__(self, argument):

        if not callable(argument):
            msg = "You cannot use autotiming measurement directly, use either " \
                  "with a context manager or a decorator"
            raise RuntimeError(msg)

        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class CounterMeasuringPrimitive(MeasuringPrimitive):
    # TODO: This one for having increment/decrement on it on it's own

    def increment(self, by_how_much):
        pass

    def decrement(self, by_how_much):
        pass


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

        # If this is disabled, mock the methods and bail
        if not enabled:
            self._mock_measurement_methods()
            return

        # Setup stats connection
        self._statsd = DogStatsd(
            host=statsd_host, port=statsd_port,
            constant_tags=_dict_as_statsd_tags(default_tags)
        )

        # TODO: add metric specific prefixes (low pri, probably)

        # Add measurement methods
        self.increment = self._wrap_measurement_method(
            self._statsd.increment, default_value=1, prefix=prefix
        )
        self.decrement = self._wrap_measurement_method(
            self._statsd.decrement, default_value=1, prefix=prefix
        )
        self.gauge = self._wrap_measurement_method(
            self._statsd.gauge, prefix=prefix
        )

        self.timing = self._wrap_measurement_method(
            self._statsd.timing, prefix=prefix
        )
        self.set = self._wrap_measurement_method(
            self._statsd.set, prefix=prefix
        )

        # We had these before, check what we did there
        # TODO: counter - attach specific ctx manager / deco , replace with 'count' and add incr/decr on it
        # TODO: autotiming - attach specific ctx manager / deco to allow for "automatic" timing

    def _wrap_measurement_method(
        self, func, prefix, default_value=None
    ):
        """
        We need to wrap the singular measurement function with our
        MeasuringPrimitive class, so that we can support various interfaces
        on top of it

        :param function func: The function to be wrapped
        :param string prefix: Common metric prefix
        :param any default_value: Default value for the metric
        :return function: The partial to be called on the MeasuringPrimitive
            constructor
        """
        return functools.partial(
            MeasuringPrimitive, func, prefix, default_value
        )

    def _mock_measurement_methods(self):
        """
        Mocks the measurement methods, so that the code still works if we
        disable the reporting. This way the code does not complain, and we
        keep the testability of the inner wrappers, because no need to propagate
        disabled state
        """
        class MockContext(ContextDecorator):

            def __init__(self, *args, **kwargs):
                pass

            def measure(self, value=None):
                pass

            def __call__(self, argument):
                if callable(argument):
                    return super().__call__(
                        functools.partial(argument, measurement=self)
                    )
                # Do nothing otherwise

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        self.increment = MockContext
        self.decrement = MockContext
        self.gauge = MockContext
        self.timing = MockContext
        self.set = MockContext


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
