# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from oozer.common.vendor_data import add_vendor_data


class TestingVendorDataAugmentationTools(TestCase):

    def test_add_new_vendor_block(self):

        data = {
            'a': 1
        }
        data_should_be = {
            'a': 1,
            '__oprm': {
                'id': 5
            }
        }

        data_actual = add_vendor_data(data, id=5)

        assert data_actual is data, 'we did not repackage the instance. Same instance'
        assert data_actual == data_should_be

    def test_update_existing_vendor_block(self):

        data = {
            'a': 1,
            '__oprm': {
                'id': 5
            }
        }
        data_should_be = {
            'a': 1,
            '__oprm': {
                'id': 5,
                'extra_attr': 7
            }
        }

        data_actual = add_vendor_data(data, extra_attr=7)

        assert data_actual is data, 'we did not repackage the instance. Same instance'
        assert data_actual == data_should_be
