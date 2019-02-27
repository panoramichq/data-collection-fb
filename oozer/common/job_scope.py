from datetime import datetime, date

from common.id_tools import generate_id


class JobScope:
    """
    A context object serving as dumping ground for all information about a given
    job, be it normative or effective

    """

    # System information
    sweep_id = None

    # Job ID components parsed
    namespace = 'fb'  # used for generating Job IDs from this data

    ad_account_id = None  # type: str or None
    ad_account_timezone_name = None  # type: str or None

    entity_id = None  # type: str or None
    entity_type = None  # type: str or None

    report_type = None  # type: str
    report_variant = None  # type: str or None

    range_start = None  # type: str or datetime or date or None
    range_end = None  # type: str or datetime or date or None

    tokens = None  # type: list

    score = None  # type: int

    # Indicates that this is a synthetically created instance of JobScope
    # (likely by the worker code to indicate some sub-level of work done)
    # and not the original JobScope pushed out by Sweep Looper that triggered the task
    # Setting this flag is important for part of the system that monitors
    # the jobs status stream and makes decisions about when to quit the cycle.
    # Successful derivative JobScope objects will be ignored by that part of the system
    # as if it counted them, the "successful" count would be greatly exaggerated
    is_derivative = False

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.to_dict() == other.to_dict()
        return False

    def __repr__(self):
        return serialize_class_with_props(self)
        # return f'<JobScope {self.sweep_id}:{self.job_id}>'

    def __str__(self):
        return f'<JobScope {self.sweep_id}:{self.job_id}>'

    def update(self, *args, **kwargs):
        for arg in args:
            self.__dict__.update(arg)
        self.__dict__.update(kwargs)

    @property
    def token(self):
        return self.tokens[0] if self.tokens else None

    def to_dict(self):
        return {
            k:v
            for k, v in self.__dict__.items()
            if v is not None
        }

    @property
    def job_id(self):
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
