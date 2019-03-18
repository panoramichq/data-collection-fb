import pytest
import gevent

from common.timeout import timeout


def test_timeout_exception():
    """Test function times out properly."""
    def test_func():
        gevent.sleep(10)

    timeout_factory = timeout(1)
    wrapped_func = timeout_factory(test_func)
    with pytest.raises(gevent.Timeout):
        wrapped_func()


def test_timeout_success():
    """Test function times out properly."""
    def test_func():
        gevent.sleep(1)

    timeout_factory = timeout(10)
    wrapped_func = timeout_factory(test_func)
    wrapped_func()
