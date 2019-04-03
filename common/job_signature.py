from collections import namedtuple
from typing import Any, Tuple, Dict


JobSignature = namedtuple(
    'JobSignature',
    [
        'job_id',
        'args',
        'kwargs'
    ]
)

JobSignature.__doc__ += """
Represents serialized call signature of some task

:param job_id:
:param tuple args:
:param dict kwargs:
"""

# namedtuples are cool and all, but overriding initializer on them is annoying.
# This is a convenience init method where instead of having to communicate
# args and kwargs as tuple and dict, one can leverage *args, **kwargs.
JobSignature.bind = classmethod(
    lambda cls, job_id, *args, **kwargs: cls(job_id, args, kwargs)
) # type: (str, *Any, **Any) -> JobSignature
