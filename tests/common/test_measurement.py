# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common.measurement import Measure


class TestDirectMeasurements(TestCase):

    def test_direct_measuring(self):

        Measure.increment('increment.direct', 3)

