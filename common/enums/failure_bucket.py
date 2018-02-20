class FailureBucket:
    """
    Enums for thematic grouping of failures into meaningful
    (from stand point of being actionable in distinctly different ways)
    buckets.

    Used largely by Prioritizer and partly by Sweep Looper (for early exit).
    """

    # Too hazy to be directly actionable.
    # Log and try again, i guess.
    Other = 1

    # any of the throttling errors (App ID, Ad Account ID, User ID)
    # Typical action here is to wait it out.
    Throttling = 100

    # errors indicating that our use of API causes vendor to error out
    # due to very large computation / memory / time needs to process such request
    # Typical action here is to form requests such that payloads become smaller.
    TooLarge = 1000
