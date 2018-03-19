"""
The purpose of this module is to handle mapping of various naming conventions we
are giving to actionable items in the system back and forth.
"""
from collections import namedtuple
from datetime import date, datetime
from itertools import zip_longest


NAMESPACE = 'fb'


fields = [
    "namespace",
    "ad_account_id",
    "entity_type",
    "entity_id",
    "report_type",
    "report_variant",
    "range_start",
    "range_end",
]


JobIdParts = namedtuple('JobIdParts', fields)


def generate_id(
    ad_account_id=None,
    entity_type=None,
    entity_id=None,
    report_type=None,
    report_variant=None,
    range_start=None,
    range_end=None,
    namespace=NAMESPACE,
    **ingored
):
    """
    Generate a string that uniquely identifies an entity, a report type, a job
    (combination of entity and report type)..

    ID Format::

        "{namespace}:{ad_account_id}" +
        ":{entity_type}:{entity_id}" +
        ":{report_type}:{report_variant}" +
        ":{range_start}:{range_end}"

    Examples::

        # Entity ID
        fb:121:C:1
        fb:121:AS:2
        fb:121:A:3

        # A job tasked with fetch of entities
        fb:121:::entities:C  # per AA 121, pull all C entities data
        fb:121:::entities:AS  # per AA 121, pull all AS entities data
        fb:121:::entities:A  # per AA, pull all A entities data
        fb:123:C:10:entities:A  # per C 10, pull all A entities data

        # A job tasked with fetch of given day's per-hour breakdown
        fb:123:::dayhour:C:2018-02-03  # per AA 123, level C, breakdown by hour for date
        fb:123:C:10:dayhour::2018-02-03  # per C 10, breakdown by hour for date

    The "optionality" of given argument is expressed by omitting the value, but
    having the colon in there. If something is optional, it means it will take
    "parent" value from the hierarchy. This applies up to the "family of report"
    switch.
    """
    assert namespace, 'Namespace is a required parameter for id generation'

    if ad_account_id is None:
        ad_account_id = ''
    if entity_type is None:
        entity_type = ''
    if entity_id is None:
        entity_id = ''
    if report_type is None:
        report_type = ''
    if report_variant is None:
        report_variant = ''
    if range_start is None:
        range_start = ''
    elif isinstance(range_start, (date, datetime)):
        range_start = range_start.strftime('%Y-%m-%d')
    if range_end is None:
        range_end = ''
    elif isinstance(range_end, (date, datetime)):
        range_end = range_end.strftime('%Y-%m-%d')

    return (
        f"{namespace}:{ad_account_id}" +
        f":{entity_type}:{entity_id}" +
        f":{report_type}:{report_variant}" +
        f":{range_start}:{range_end}"
    ).strip(':')


def parse_id(job_id, fields=fields):

    data = {
        key: value or None
        for key, value in zip_longest(fields, job_id.split(':')[:len(fields)])
    }

    to_date = lambda value: datetime.strptime(value, '%Y-%m-%d').date() if value else value

    # range_start, range_end are not guaranteed to be date objects,
    # but in majority of cases, they can be.
    # When they are not dates, they are some object id
    # indicating range of IDs to scan (say, Scope ID)
    for key in ['range_start', 'range_end']:
        try:
            data[key] = to_date(data[key])
        except ValueError:
            pass

    return data
