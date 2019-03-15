# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase
from unittest.mock import patch

from common import bugsnag
from common.util import convert_class_with_props_to_str


# because it's declared global.y, it's pickle-able
class GlobalBlahForTests:
    def __repr__(self):
        return convert_class_with_props_to_str(GlobalBlahForTests)


class TestingSafeEncoder(TestCase):
    def test_string(self):
        data = bugsnag._make_data_safe_for_serialization('Some Text')
        assert data == 'Some Text'

    def test_array(self):
        data = bugsnag._make_data_safe_for_serialization([1, 2, 3])
        assert data == [1, 2, 3]

    def test_complex_nested(self):
        data = bugsnag._make_data_safe_for_serialization({'a': [1, 2, 3]})
        assert data == {'a': [1, 2, 3]}

    def test_some_non_jsonable_but_pickleable_instance(self):
        instance = GlobalBlahForTests()

        data = bugsnag._make_data_safe_for_serialization({'a': instance})
        assert data == {
            'a': repr(instance)
            + ';data:application/python-pickle;base64,'
            + 'gANjdGVzdHMuY29tbW9uLnRlc3RfYnVnc25hZwpHbG9iYWxCbGFoRm9yVGVzdHMKcQApgXEBLg=='
        }

    def test_some_non_jsonable_and_non_pickleable_instance(self):
        # because it's declared in-line, it's unpickle-able
        class LocalBlahForTests:
            pass

        instance = LocalBlahForTests()

        data = bugsnag._make_data_safe_for_serialization({'a': instance})
        assert data == {'a': repr(instance)}


def test_notify_no_severity_specified():
    with patch('common.bugsnag.bugsnag') as mock_bugsnag:
        exc = Exception('test error')
        bugsnag.BugSnagContextData.notify(exc, key='value')
        mock_bugsnag.notify.assert_called_once_with(exc, meta_data={'extra_data': {"key": "value"}}, severity='error')


def test_notify_warning_severity_specified():
    with patch('common.bugsnag.bugsnag') as mock_bugsnag:
        exc = Exception('test error')
        bugsnag.BugSnagContextData.notify(exc, key='value', severity=bugsnag.SEVERITY_WARNING)
        mock_bugsnag.notify.assert_called_once_with(exc, meta_data={'extra_data': {"key": "value"}}, severity='warning')


def test_notify_error_severity_specified():
    with patch('common.bugsnag.bugsnag') as mock_bugsnag:
        exc = Exception('test error')
        bugsnag.BugSnagContextData.notify(exc, severity=bugsnag.SEVERITY_ERROR, key='value')
        mock_bugsnag.notify.assert_called_once_with(exc, meta_data={'extra_data': {"key": "value"}}, severity='error')
