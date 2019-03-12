class CollectionError:
    """Unrecoverable error thrown when a collection job fails."""

    def __init__(self, partial_datapoint_count: int):
        self.partial_datapoint_count = partial_datapoint_count
