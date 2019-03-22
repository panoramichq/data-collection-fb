import pytest
import gevent

from common.timeout import timeout


def test_timeout_exception():
    """Test function times out properly."""

    def test_func(a, b, c=None):
        assert (a, b, c) == (1, 2, 3)
        gevent.sleep(10)

    timeout_factory = timeout(1)
    wrapped_func = timeout_factory(test_func)
    with pytest.raises(gevent.Timeout):
        wrapped_func(1, 2, c=3)


def test_timeout_success():
    """Test function doesn't time out prematurely."""

    def test_func(a, b, c=None):
        assert (a, b, c) == (1, 2, 3)
        gevent.sleep(1)

    timeout_factory = timeout(10)
    wrapped_func = timeout_factory(test_func)
    wrapped_func(1, 2, c=3)
