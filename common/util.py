import re

from typing import Match
from facebook_business.exceptions import FacebookError


def convert_class_with_props_to_str(class_instance):
    new_dict = {
        key: class_instance.__dict__[key]
        for key in class_instance.__dict__
        if key[:2] != '__' and not callable(class_instance.__dict__[key])
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
