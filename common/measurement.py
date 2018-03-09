import functools
from typing import Callable, List

from datadog import DogStatsd

from config import build, measurement


def _dict_as_statsd_tags(tags):
    """
    Reformat tags to somethign statsd understands

    :param dict tags: Tags in dictionary form
    :return list:
    """
    return [ f'{tag}:{tag_value}' for tag, tag_value in tags.items()]


class MeasuringPrimivite:
    """
    A wrapper for measuring functions that adds some application wide stuff.

    Namely:
        - common metric prefix
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

    def __init__(
        self, measure_function, prefix=None, default_value=None,
    ):
        self._statsd_callable = measure_function
        self._prefix = prefix
        self._default_value = default_value

    def __call__(self, metric, value=None, tags=None, sample_rate=1):
        """
        Send a metric directly


        TODO: fork this out to ctx managers
        """

        # We may have empty tags
        tags = tags or {}

        # Apply default value if needed
        value = value or self._default_value

        # Add metric prefix
        metric = '.'.join([self._prefix, metric])

        print("sending metric: " + metric )
        self._statsd_callable(
            metric, value, _dict_as_statsd_tags(tags), sample_rate
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
        # If this is disabled, mock the methods and bail
        if not enabled:

            def mock_measure(metric, value=None, tags=None, sample_rate=1):
                pass

            self.increment = mock_measure
            self.gauge = mock_measure
            self.timing = mock_measure
            self.set = mock_measure
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
        return MeasuringPrimivite(
            func, prefix=prefix, default_value=default_value,
        )


# Instance of the measuring tools injected with configuration options
Measure = MeasureWrapper(
    enabled=measurement.ENABLE,
    statsd_host=measurement.STATSD_SERVER,
    statsd_port=measurement.STATSD_PORT,
    prefix=measurement.METRIC_PREFIX,
    default_tags={
        'build_id': build.BUILD_ID,
        'commit_id': build.COMMIT_ID,
    }
)
