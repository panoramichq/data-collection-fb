# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common import bugsnag

# because it's declared global.y, it's pickle-able
class GlobalBlahForTests:
    pass


class TestingSafeEncoder(TestCase):

    def test_string(self):
        data = bugsnag._make_data_safe_for_serialization('Some Text')
        assert data == 'Some Text'

    def test_array(self):
        data = bugsnag._make_data_safe_for_serialization([1,2,3])
        assert data == [1,2,3]

    def test_complex_nested(self):
        data = bugsnag._make_data_safe_for_serialization({
            'a': [1,2,3]
        })
        assert data == {
            'a': [1,2,3]
        }

    def test_some_non_jsonable_but_pickleable_instance(self):

        instance = GlobalBlahForTests()

        data = bugsnag._make_data_safe_for_serialization({
            'a': instance
        })
        assert data == {
            'a': 'data:application/python-pickle;base64,gANjdGVzdHMuY29tbW9uLnRlc3RfYnVnc25hZwpHbG9iYWxCbGFoRm9yVGVzdHMKcQApgXEBLg=='
        }

    def test_some_non_jsonable_and_non_pickleable_instance(self):

        # because it's declared in-line, it's unpickle-able
        class LocalBlahForTests:
            pass

        instance = LocalBlahForTests()

        data = bugsnag._make_data_safe_for_serialization({
            'a': instance
        })
        assert data == {
            'a': repr(instance)
        }
