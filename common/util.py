import re

from sys import getsizeof
from itertools import chain
from collections import deque

from typing import Match, Optional, List
from facebook_business.exceptions import FacebookError


def convert_class_with_props_to_str(class_instance, filter_props: Optional[List[str]] = None):
    final_filter_props = filter_props or []
    new_dict = {
        key: class_instance.__dict__[key]
        for key in class_instance.__dict__
        if key[:2] != '__' and not callable(class_instance.__dict__[key]) and key not in final_filter_props
    }

    return f'<{class_instance.__class__.__name__} {new_dict}>'


def sub_asterisks(m: Match) -> str:
    """Substitute match group with asterisks."""
    return '*' * len(m.group())


def redact_access_token_from_str(value: str) -> str:
    """Remove access token from string."""
    return re.sub(r'(?<=access_token=)\w+?(?=&)', sub_asterisks, value)


def redact_access_token(e: Exception) -> Exception:
    """Remove access token from exception message."""
    if not isinstance(e, FacebookError):
        return e
    e.args = (redact_access_token_from_str(str(e.args[0])),)
    return e


def total_size(o):
    """Returns the approximate memory footprint an object and all of its contents."""

    def dict_handler(d):
        return chain.from_iterable(d.items())

    all_handlers = {tuple: iter, list: iter, deque: iter, dict: dict_handler, set: iter, frozenset: iter}
    seen = set()
    default_size = getsizeof(0)

    def sizeof(o):
        if id(o) in seen:
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)
