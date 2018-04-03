from common.enums.failure_bucket import FailureBucket


class JobStatus:
    """
    A class to represent job states (stage ids) for given jobs. Inherit and add
    your arbitrary states.

    The guideline is:

    - use positive numbers for "good" states
    - use negative numbers for "error" states

    You can write the job statues in two ways:

    either simple:

    MyStatus = 123

    or, compound with explicitly stated failure bucket, which is up to the
    task to decide, like this:

    ErrorStatus = -123, FailureBucket.Throttling


    You do not need to state FailureBucket.Other, as that is assumed to be the
    general case
    """

    # Any job considered started is represented by 100
    Start = 100
    # Any job considered done is represented by 1000
    Done = 1000

    # Any generic failure is represented by -1000 (negative Done)
    GenericError = -1000


class ExternalPlatformJobStatus(JobStatus):
    """
    Use this to communicate to give status reporter enough information to
    figure out what the stage id means in terms of failures
    """

    # Progress states
    DataFetched = 200
    InColdStore = 500

    # Various error states

    # Indicates FB hinting us that API call failed because we might have asked for too much data
    TooMuchData = -500
    # FB API throttling kicked in. At this point we don't diff if its App vs AdAccount vs User throttling.
    # TODO: plan for splitting this into 3: App | AdAccount | User
    ThrottlingError = -700
    GenericPlatformError = -900

    failure_bucket_map = {
        DataFetched: FailureBucket.WorkingOnIt,
        JobStatus.Done: FailureBucket.Success,
        TooMuchData: FailureBucket.TooLarge,
        ThrottlingError: FailureBucket.Throttling,
        GenericPlatformError: FailureBucket.Other,
        JobStatus.GenericError: FailureBucket.Other
    }
