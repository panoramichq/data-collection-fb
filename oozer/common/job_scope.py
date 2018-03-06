from common.id_tools import generate_id


class JobScope:
    """
    A context object serving as dumping ground for all information about a given
    job, be it normative or effective

    """

    METADATA_STRING_FIELDS = {
        'platform',
        'ad_account_id',
        'entity_id',
        'entity_type',
        'report_type',
        'report_variant',
    }

    METADATA_DATE_FIELDS = {
        'range_start',
        'range_end',
    }

    METADATA_FIELDS = METADATA_STRING_FIELDS | METADATA_DATE_FIELDS

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

    @property
    def metadata(self):
        """
        Assemble metadata from the job scope, dumping relevant information

        :return dict: A dict of metadata (key/value)
        """
        metadata = {
            x: str(getattr(self, x)) for x in self.METADATA_STRING_FIELDS
        }

        for date_field in self.METADATA_DATE_FIELDS:
            metadata[date_field] = \
                getattr(self, date_field).strftime('%Y-%m-%d') \
                if getattr(self,date_field, None) \
                else str(None)

        return metadata
