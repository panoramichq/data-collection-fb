import functools
from contextlib import ContextDecorator, contextmanager

from datadog import DogStatsd

from config import build, measurement


def _dict_as_statsd_tags(tags):
    """
    Reformat tags to somethign statsd understands

    :param dict tags: Tags in dictionary form
    :return list:
    """
    return [f'{tag}:{tag_value}' for tag, tag_value in tags.items()]


class MeasuringPrimivite:
    """
    A wrapper for measuring functions that adds some application wide stuff.

    Namely:
        - common metric tags for slicing the values

    Also, the wrapper gives you multiple ways to use particular measurement:

    - directly:
        For simple measurement one-off calls

        you call Measure.increment('mymetric', value) and set the value

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

    class MeasurementContextWrapper(ContextDecorator):
        """
        Wrapper for our function, that
        """

        def __init__(self, measure_funciton, default_value, metric, tags, sample_rate):
            self._statsd_func = measure_funciton
            self._default_value = default_value
            self._metric = metric
            self._tags = tags
            self._sample_rate = sample_rate

        def measure(self, value=None):
            """
            Initiate the actual measurement

            :param any value: Any value to be sent along to statsd
            """
            # Apply default value if needed
            value = value or self._default_value

            print("Sending value: " + str(value))
            self._statsd_func(
                self._metric,
                value,
                _dict_as_statsd_tags(self._tags),
                self._sample_rate
            )

        def __call__(self, func):
            """
            Inject the measurement wrapper to the function
            """
            return super().__call__(functools.partial(func, measurement=self))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    def __init__(
        self, measure_function, prefix=None, default_value=None,
    ):
        """
        Instrument the function with prefix and default value
        """
        self._statsd_func = measure_function
        self._prefix = prefix
        self._default_value = default_value

    def __call__(self, metric, tags=None, sample_rate=1):
        """
        Wrap so that this can be used as context manager or method decorator

        :return ContextManager
        """

        # We may have empty tags
        tags = tags or {}

        # Add metric prefix
        metric = '.'.join([self._prefix, metric])

        return self.MeasurementContextWrapper(
            self._statsd_func, self._default_value,
            metric, tags, sample_rate,
        )


class MeasureWrapper:
    """
    Wraps connection to the statsd server and creates wrapped measuring methods
    on top of the class.

    Provided you create an instance measure = MeasureWrapper(*args), you can
    then use it like this:

    measure.increment('metric', value)

    or with context managers:

    with measure.increment('metric') as m:
        m(value)

    or with function decorators

    @measure.increment('metric')
    def my_method(blah, measuring_context):
        measuring_context(value)

    """

    _statsd = None

    increment = None   # type: MeasuringPrimivite
    gauge = None       # type: MeasuringPrimivite
    timing = None      # type: MeasuringPrimivite
    set = None         # type: MeasuringPrimivite

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

        # Add measurement methods
        self.increment = self._wrap_measurement_method(
            self._statsd.increment, default_value=1, prefix=prefix
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

    def _wrap_measurement_method(
        self, func, prefix, default_value=None
    ):
        """
        We need to wrap the singular measurement function with

        :param func:
        :param prefix:
        :param default_value:
        :return:
        """
        return MeasuringPrimivite(
            func, prefix=prefix, default_value=default_value,
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

            def __call__(self, func):
                return super().__call__(
                    functools.partial(func, measurement=self)
                )

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        self.increment = MockContext
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
