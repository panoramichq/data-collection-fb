from datetime import datetime, date
from typing import List, Union

from common.id_tools import generate_id
from common.util import convert_class_with_props_to_str


class JobScope:
    """
    A context object serving as dumping ground for all information about a given
    job, be it normative or effective
    """

    # System information
    sweep_id: str = None

    # Job ID components parsed
    namespace: str = 'fb'  # used for generating Job IDs from this data

    ad_account_id: str = None
    ad_account_timezone_name: str = None

    entity_id: str = None
    entity_type: str = None

    report_type: str = None
    report_variant: str = None

    range_start: Union[datetime, date] = None
    range_end: Union[datetime, date] = None

    tokens: List[str] = None

    score: int = None
    running_time: int = None
    datapoint_count: int = None

    # Indicates that this is a synthetically created instance of JobScope
    # (likely by the worker code to indicate some sub-level of work done)
    # and not the original JobScope pushed out by Sweep Looper that triggered the task
    # Setting this flag is important for part of the system that monitors
    # the jobs status stream and makes decisions about when to quit the cycle.
    # Successful derivative JobScope objects will be ignored by that part of the system
    # as if it counted them, the "successful" count would be greatly exaggerated
    is_derivative: bool = False

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.to_dict() == other.to_dict()
        return False

    def __getitem__(self, item):
        return self.__dict__.get(item)

    def __repr__(self):
        return convert_class_with_props_to_str(self)

    def __str__(self):
        return f'<JobScope {self.sweep_id}:{self.job_id}>'

    def update(self, *args, **kwargs):
        for arg in args:
            self.__dict__.update(arg)
        self.__dict__.update(kwargs)

    @property
    def token(self) -> str:
        return self.tokens[0] if self.tokens else None

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @property
    def job_id(self) -> str:
        return generate_id(
            ad_account_id=self.ad_account_id,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            report_type=self.report_type,
            report_variant=self.report_variant,
            range_start=self.range_start,
            range_end=self.range_end,
            namespace=self.namespace,
        )
