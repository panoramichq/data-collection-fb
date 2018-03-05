"""
To ward off evil (mis)spelling spirits, using centralized collection of labels
to be used for labeling all reports
"""

class ReportType:
    """
    There are 2 types of report type labels we use in this system:
    - normative, end-result-indicative report type labels, and
    - effective, actual report types used to achieve the normative result type labels.

    Examples of this split:
        normative: get lifetime stats for this AdSet
        effective: use AA/insights edge, group by AdSet to get lifetime numbers for AdSets

        normative: get hourly data for Ad for January 1, 2000, hour 3
        effective: use AA/insights edge, group by Ad, breakdown by our for days Jan 1, 2 and 3

    In this enum we list (at one level) all normative and effective report type labels, without
    particular indication in this enum of which one is which. Expectation Builder will use its
    own logic to form its normative report types and associate alternatives (possible effective
    report types) with that normative type, all specific to Entity level or situation.
    """

    # These usually act as normative report types - indicate the end result to be achieved

    # note the pattern in formation of the values of the enum:
    #  '{range of data coverage - lifetime|day|hour}{level of breakdown}'
    # enum attr name is shorter for convenience, which may be a problem
    # if, say, we can have same breakdown but different range
    # (say, both, day and lifetime range for age_gender breakdown)
    # In that case, refactoring of attr names is easy without messing with enum values.
    # An effort is made to avoid using separator-type characters (: | _ etc) in the values
    # because these are often used as fragments in compound IDs that use separator type characters.

    console = 'console' # reporty type that asks console about active

    entities = 'entities'  # multiple entities per some parent

    lifetime = 'lifetime'
    # day_hour is a bastardisation of
    # actual `hour` normative task (meaning we actually, eventually
    # store and use this data in per-hour slices in Cold Store and elsewhere)
    # This term moves away from "normative" towards
    # "effective" task name. It's an acceptance of fact that:
    #  1) tracking each hour (as opposed to a Day with hourly breakdown) is a bitch
    #  2) We are unlikely ever to be breaking down our API hits to FB into per-hour calls
    # So, while proper normative report type here would be `hour` and
    # proper "effective" report type is `day_hour` we will just treat
    # day_hour as "normative" and expect some system down the stream to
    # break this into hour slices for storage, at which point it
    # snaps to its true normative nature.
    # This also represents a simplification for us, where
    # treatment of time and Day is same as for all other reports
    # where Day is a Day per AdAccount's timezone.
    # This makes it simpler for us because missing/extra hour that
    # occur in days for countries that observe Summer Time will be
    # ALWAYS in the middle of the per-AA-timezone Day slice of hours
    # (it's usually 2nd or 3rd hour in the day)
    # and, thus, we will not have to deal with finding specific UTC hour
    # for timezone-specific missing / extra hours if we break down on UTC days.
    # (This mostly affects Europe/London timezone)

    class Breakdown:
        hour = 'hour'
        age_gender = 'agegender'
        dma = 'dma'

    day_age_gender = f'day{Breakdown.age_gender}'
    day_dma = f'day{Breakdown.dma}'
    day_hour = f'day{Breakdown.hour}'

    ALL_DAY_BREAKDOWNS = {
        day_age_gender,
        day_dma,
        day_hour,
    }

    ALL_METRICS = ALL_DAY_BREAKDOWNS | {
        lifetime
    }
