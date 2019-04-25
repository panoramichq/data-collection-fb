class FailureBucket:
    """
    Enums for thematic grouping of failures into meaningful
    (from stand point of being actionable in distinctly different ways)
    buckets.

    Used largely by Prioritizer and partly by Sweep Looper (for early exit).
    """

    # Had to have it here because other code uses attributes from this enum
    # to build Pulse object attributes. Not happy about this
    # TODO: decouple Pulse attributes from this enum such that we don't have to stick this on "Failure"
    WorkingOnIt = -100
    Success = 0

    # Too hazy to be directly actionable.
    # Log and try again, i guess.
    Other = 1

    # any of the throttling errors (App ID, Ad Account ID, User ID)
    # Typical action here is to wait it out.
    Throttling = 100
    UserThrottling = 200
    AdAccountThrottling = 300
    ApplicationThrottling = 400
    InaccessibleObject = 550

    # errors indicating that our use of API causes vendor to error out
    # due to very large computation / memory / time needs to process such request
    # Typical action here is to form requests such that payloads become smaller.
    TooLarge = 1000


# enum values are ints. Not a convenient thing to use as
# keys / attr names. For convenience of being able to use
# pretty names as keys:
FailureBucket.attr_name_enum_value_map = {
    k: v
    for k, v in FailureBucket.__dict__.items()
    if not k.startswith('_') and not callable(v) and not isinstance(v, classmethod)
}
