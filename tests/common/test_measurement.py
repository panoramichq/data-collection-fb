# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.measurement import MeasureWrapper
from config import measurement, build

# TODO: Ugly as hell, fix later

# TODO: Mock out actual statsd calls and verify it does what it's supposed to do


class IncrMeasurementsWork:
    """
    No tests really, just verifying this thing works as expected
    """

    def test_incr_direct_measuring(self):
        self.measure.increment('increment.direct')(1)

    def test_incr_as_ctx_manager(self):

        with self.measure.increment('increment.ctx') as measurement:
            measurement(5)

    def test_incr_as_decorator(self):

        @self.measure.increment('increment.deco', tags={'my': 'custom_tag'})
        def some_func(measurement):
            measurement(10)

        some_func()


class DecrMeasurementsWork:
    """
    No tests really, just verifying this thing works as expected
    """

    def test_decr_direct_measuring(self):
        self.measure.decrement('decrement.direct')(1)

    def test_decr_as_ctx_manager(self):

        with self.measure.decrement('decrement.ctx') as measurement:
            measurement(5)

    def test_decr_as_decorator(self):

        @self.measure.decrement('decrement.deco', tags={'my': 'custom_tag'})
        def some_func(measurement):
            measurement(10)

        some_func()


class GaugeMeasurementsWork:
    """
    No tests really, just verifying this thing works as expected
    """

    def test_gauge_direct_measuring(self):
        self.measure.gauge('gauge.direct')(1)

    def test_gauge_as_ctx_manager(self):

        with self.measure.gauge('gauge.ctx') as measurement:
            measurement(5)

    def test_gauge_as_decorator(self):

        @self.measure.gauge('gauge.deco', tags={'my': 'custom_tag'})
        def some_func(measurement):
            measurement(10)

        some_func()


class SetMeasurementsWork:
    """
    No tests really, just verifying this thing works as expected
    """

    def test_set_direct_measuring(self):
        self.measure.set('set.direct')(1)

    def test_set_as_ctx_manager(self):

        with self.measure.set('set.ctx') as measurement:
            measurement(5)

    def test_set_as_decorator(self):

        @self.measure.set('set.deco', tags={'my': 'custom_tag'})
        def some_func(measurement):
            measurement(10)

        some_func()


class TimingMeasurementsWork:
    """
    No tests really, just verifying this thing works as expected
    """

    def test_timing_direct_measuring(self):
        self.measure.timing('timing.direct')(1)

    def test_timing_as_ctx_manager(self):

        with self.measure.timing('timing.ctx') as measurement:
            measurement(5)

    def test_timing_as_decorator(self):

        @self.measure.timing('timing.deco', tags={'my': 'custom_tag'})
        def some_func(measurement):
            measurement(10)

        some_func()


class TestMeasurementEnabledWorks(
    IncrMeasurementsWork,
    DecrMeasurementsWork,
    GaugeMeasurementsWork,
    SetMeasurementsWork,
    TimingMeasurementsWork,
    TestCase
):

    def setUp(self):
        self.measure = MeasureWrapper(
            enabled=True,
            statsd_host=measurement.STATSD_SERVER,
            statsd_port=measurement.STATSD_PORT,
            prefix=measurement.METRIC_PREFIX,
            default_tags={
                'build_id': build.BUILD_ID,
                'commit_id': build.COMMIT_ID,
            }
        )


class TestMeasuremenDisabledWorks(
    IncrMeasurementsWork,
    DecrMeasurementsWork,
    GaugeMeasurementsWork,
    SetMeasurementsWork,
    TimingMeasurementsWork,
    TestCase
):

    def setUp(self):
        self.measure = MeasureWrapper(
            enabled=False,
            statsd_host=measurement.STATSD_SERVER,
            statsd_port=measurement.STATSD_PORT,
            prefix=measurement.METRIC_PREFIX,
            default_tags={
                'build_id': build.BUILD_ID,
                'commit_id': build.COMMIT_ID,
            }
        )
