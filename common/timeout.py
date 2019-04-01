import functools
import logging

from typing import Callable, Any
from gevent import Timeout

from oozer.common.errors import TimeoutException

logger = logging.getLogger(__name__)


def timeout(seconds: int) -> [[Callable], Callable]:
    """Create decorator that wraps with seconds timeout."""

    def _timeout(func: Callable) -> Callable:
        """Wrap func in a gevent timeout block."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                with Timeout(seconds):
                    return func(*args, **kwargs)
            except Timeout as e:
                msg = f'Timed out running function {func.__name__}'
                logger.exception(msg)
                raise TimeoutException(msg) from e

        return wrapper

    return _timeout
