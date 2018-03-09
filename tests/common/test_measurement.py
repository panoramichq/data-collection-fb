# must be first, as it does event loop patching and other "first" things
import unittest

from tests.base.testcase import TestCase

from common.measurement import Measure


class TestDirectMeasurements(TestCase):

    @unittest.skip
    def test_direct_measuring(self):
        val = Measure.increment('increment.direct', 3)
        raise Exception(val)

    def test_as_ctx_manager(self):

        with Measure.increment('increment.ctx') as measurement:
            measurement.measure(5)

    def test_as_decorator(self):

        @Measure.increment('increment.deco', tags={'my': 'custom_tag'})
        def some_func(measurement, *args, **kwargs):
            measurement.measure(10)

        some_func()
