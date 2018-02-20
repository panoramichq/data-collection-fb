
class JobScope:
    """
    A context object serving as dumping ground for all information about a given
    job, be it normative or effective

    """

    platform = 'facebook'

    tokens = None

    ad_account_id = None

    report_type = None
    report_id = None
    metadata = None

    report_time = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

        # Normalize few things
        self.metadata = kwargs.get('metadata', {})

    @property
    def token(self):
        return self.tokens[0]

    def to_dict(self):
        """
        Celery will need this

        :return dict:
        """
        return self.__dict__.copy()
