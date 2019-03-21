import functools
import logging

from typing import Callable, Any
from gevent import Timeout

from common.bugsnag import BugSnagContextData

logger = logging.getLogger(__name__)


def timeout(seconds: int) -> [[Callable], Callable]:
    """Create decorator that wraps with seconds timeout."""

    def _timeout(func: Callable) -> Callable:
        """Wrap func in a gevent timeout block."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                with Timeout(seconds):
                    return func(*args, *kwargs)
            except Timeout as e:
                BugSnagContextData.notify(e)
                logger.exception(f"Timed out running function {func.__name__}")
                raise

        return wrapper

    return _timeout
