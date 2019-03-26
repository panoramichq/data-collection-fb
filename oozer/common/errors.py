class CollectionError(Exception):
    """Unrecoverable error thrown when a collection job fails."""

    def __init__(self, inner: Exception, partial_datapoint_count: int):
        self.inner = inner
        self.partial_datapoint_count = partial_datapoint_count


class TimeoutException(Exception):

    pass
