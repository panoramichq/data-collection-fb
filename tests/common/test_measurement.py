# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
import time

from common.measurement import MeasureWrapper
from config import measurement, build

# TODO: Mock out actual statsd calls and verify it does what it's supposed to do
# TODO: Run for disabled env too


class BaseMeasureTestCase(TestCase):

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

    def _construct_measure(self, mtype, subtype, *args, **kwargs):
        """
        A helper to construct measures and give them the rights tags

        :param string mtype: A type that needs to be available on the wrapper
            (increment, decrement etc.)
        :param string subtype: Just a tag, that should be unique in the test
            case, the easiest way is to say what is the invocation method,
            the tests use direct, ctx, deco
        :return MeasurementWrapper: The wrapper for given measurement
        """
        method = getattr(self.measure, mtype)
        return method('.'.join([mtype, subtype]), *args, **kwargs)

    def _test_direct_simple(
        self, mtype, subtype='direct', value=1, *args, **kwargs
    ):
        measure = self._construct_measure(mtype, subtype, *args, **kwargs)
        measure(value)
        return measure

    def _test_context_manager_simple(
        self, mtype, subtype='ctx', value=5, *args, **kwargs
    ):
        with self._construct_measure(mtype, subtype, *args, **kwargs) as measure:
            measure(value)
        return measure

    def _test_decorator_simple(
            self, mtype, subtype='deco', value=10, *args, **kwargs
    ):
        @self._construct_measure(mtype, subtype, bind=True, *args, **kwargs)
        def some_func(measure):
            measure(value)
            return measure

        return some_func()


class TestIncrMeasurements(BaseMeasureTestCase):
    """
    No tests really, just verifying this thing works as expected
    """

    def test_direct_measuring(self):
        measure = self._test_direct_simple('increment')

    def test_as_ctx_manager(self):
        measure = self._test_context_manager_simple('increment')

    def test_as_decorator(self):
        measure = self._test_decorator_simple('increment')


class TestDecrMeasurements(BaseMeasureTestCase):
    """
    No tests really, just verifying this thing works as expected
    """

    def test_direct_measuring(self):
        measure = self._test_direct_simple('decrement')

    def test_as_ctx_manager(self):
        measure = self._test_context_manager_simple('decrement')

    def test_as_decorator(self):
        measure = self._test_decorator_simple('decrement')


class TestGaugeMeasurements(BaseMeasureTestCase):
    """
    No tests really, just verifying this thing works as expected
    """

    def test_direct_measuring(self):
        measure = self._test_direct_simple('gauge')

    def test_as_ctx_manager(self):
        measure = self._test_context_manager_simple('gauge')

    def test_as_decorator(self):
        measure = self._test_decorator_simple('gauge')


class TestSetMeasurements(BaseMeasureTestCase):
    """
    No tests really, just verifying this thing works as expected
    """

    def test_direct_measuring(self):
        measure = self._test_direct_simple('set', tags={'my': 'custom-tag'})

    def test_as_ctx_manager(self):
        measure = self._test_context_manager_simple('set')

    def test_as_decorator(self):
        measure = self._test_decorator_simple('set')


class TestTimingMeasurements(BaseMeasureTestCase):
    """
    No tests really, just verifying this thing works as expected
    """

    def test_direct_measuring(self):
        measure = self._test_direct_simple('timing')

    def test_as_ctx_manager(self):
        measure = self._test_context_manager_simple('timing')

    def test_as_decorator(self):
        measure = self._test_decorator_simple('timing')


class TestAutotimingMeasurements(BaseMeasureTestCase):
    """
       No tests really, just verifying this thing works as expected
       """

    def test_direct_measuring_forbidden(self):
        with self.assertRaises(RuntimeError):
            self._test_direct_simple('autotiming')

    def test_as_ctx_manager(self):

        with self._construct_measure('autotiming', 'ctx') as measure:

            # Sleep for a while so we can check the value
            time.sleep(0.1)

            mid_elapsed = measure.elapsed
            self.assertGreater(mid_elapsed, 0)

            # Sleep for a while so we can check the value
            time.sleep(0.1)

            # For autotiming, this is forbidden
            with self.assertRaises(RuntimeError):
                measure(5)

        stop_time = measure.elapsed
        self.assertGreater(stop_time, mid_elapsed)

        # Sleep for a while so we can check the value
        time.sleep(0.1)

        # Timer must be stopped at this time
        self.assertEqual(stop_time, measure.elapsed)

    def test_as_decorator(self):

        @self._construct_measure('autotiming', 'deco', bind=True)
        def some_func(measure):
            # Sleep for a while so we can check the value
            time.sleep(0.1)

            mid_elapsed = measure.elapsed
            self.assertGreater(mid_elapsed, 0)

            # Sleep for a while so we can check the value
            time.sleep(0.1)

            # For autotiming, this is forbidden
            with self.assertRaises(RuntimeError):
                measure(10)

            return measure, mid_elapsed

        measure, mid_elapsed = some_func()

        stop_time = measure.elapsed
        self.assertGreater(stop_time, mid_elapsed)

        # Sleep for a while so we can check the value
        time.sleep(0.1)

        # Timer must be stopped at this time
        self.assertEqual(stop_time, measure.elapsed)


class TestCounterMeasurements(BaseMeasureTestCase):

    def test_direct_measuring_forbidden(self):
        with self.assertRaises(RuntimeError):
            self._test_direct_simple('counter')

    def test_as_ctx_manager(self):
        with self._construct_measure('counter', 'ctx') as measure:

            # Direct calls forbidden
            with self.assertRaises(RuntimeError):
                measure(1)

            # Increment/decrement by methods
            measure.increment(10)
            self.assertEqual(10, measure.total_value)

            measure.decrement(5)

            self.assertEqual(5, measure.total_value)

            # Increment/decrement by operators
            measure += 15
            self.assertEqual(20, measure.total_value)

            measure -= 5
            self.assertEqual(15, measure.total_value)

    def test_as_decorator(self):

        @self._construct_measure('counter', 'deco', bind=True)
        def some_func(measure):
            # Direct calls forbidden
            with self.assertRaises(RuntimeError):
                measure(1)

            # Increment/decrement by methods
            measure.increment(10)
            self.assertEqual(10, measure.total_value)

            measure.decrement(5)

            self.assertEqual(5, measure.total_value)

            # Increment/decrement by operators
            measure += 15
            self.assertEqual(20, measure.total_value)

            measure -= 5
            self.assertEqual(15, measure.total_value)
