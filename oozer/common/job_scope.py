from common.id_tools import generate_id


class JobScope:
    """
    A context object serving as dumping ground for all information about a given
    job, be it normative or effective

    """

    # System information
    sweep_id = None

    platform = 'facebook'

    # Job ID components parsed
    namespace = 'fb'  # used for generating Job IDs from this data
    ad_account_id = None

    entity_id = None
    entity_type = None

    report_type = None
    report_variant = None

    range_start = None
    range_end = None

    # Job performance things
    tokens = None
    metadata = None

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        # Normalize few things
        self.metadata = self.metadata or {}

    def __repr__(self):
        return f'<JobScope {self.sweep_id}:{self.job_id}>'

    def update(self, *args, **kwargs):
        for arg in args:
            self.__dict__.update(arg)
        self.__dict__.update(kwargs)

    @property
    def token(self):
        return self.tokens[0]

    def to_dict(self):
        return self.__dict__.copy()

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
