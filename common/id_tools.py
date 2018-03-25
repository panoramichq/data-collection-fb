"""
The purpose of this module is to handle mapping of various naming conventions we
are giving to actionable items in the system back and forth.
"""

from collections import namedtuple
from datetime import date, datetime
from itertools import zip_longest
from urllib.parse import quote_plus, unquote_plus

import config.application


NAMESPACE = 'fb'
ID_DELIMITER = '|'


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


universal_id_fields = [
    'component_vendor',
    'component_id',
] + fields


JobIdParts = namedtuple('JobIdParts', fields)


_id_parts_default_converter = lambda v: '' if v is None else v


def _id_parts_datetime_converter(v):

    # datetime is subclass of date, so (datetime, date) is redundant,
    # but explicit about what it's trying to do
    # If you split the logic, ensure you process datetime first, before falling into date logic.
    if isinstance(v, (datetime, date)):
        datetime_format = '%Y-%m-%d'
        if isinstance(v, datetime):
            # at this point I would say have it '%Y-%m-%dT%H:%M:%S'
            # but there was a long and partly needless discussion in #data-intake
            # about how trailing ':%M:%S' are annoying to look at in certain types of report signatures.
            # So, temporary agreement was on shortening the values to hour for the types of reports
            # that use the time component - insights by hour report:
            #   '%Y-%m-%dT%H'
            # Thus, if it's datetime, we add at least the hour to the format
            datetime_format += 'T%H'
            # Note that we are adding it always, even if hour is zero because
            # we don't know for sure if this zero means we don't care about hours
            # or if zero means it's midnight-starting slice of data.

            # For rest of values we have a shortening logic:
            if v.second or v.minute:
                datetime_format += ':%M'
            if v.second:
                datetime_format += ':%S'
        return v.strftime(datetime_format)

    # if we are here, it's NOT date/datetime
    # so kick in default logic
    return _id_parts_default_converter(v)


_id_parts_converters = {
    'range_start': _id_parts_datetime_converter,
    'range_end': _id_parts_default_converter
}


def generate_id(
    fields=fields,
    trailing_parts=None,
    **parts
):
    """
    Generate a string that uniquely identifies an entity, a report type, a job
    Output is compatible with Universal ID spec's component_scoped_id format
    https://operam.atlassian.net/wiki/spaces/EN/pages/160596078/Universal+IDs

    (combination of entity and report type)..

    ID Format::

        "{namespace}|{ad_account_id}" +
        "|{entity_type}|{entity_id}" +
        "|{report_type}|{report_variant}" +
        "|{range_start}|{range_end}"

    Examples::

        # Entity ID
        fb|121|C|1
        fb|121|AS|2
        fb|121|A|3

        # A job tasked with fetch of entities
        fb|121|||entities|C  # per AA 121, pull all C entities data
        fb|121|||entities|AS  # per AA 121, pull all AS entities data
        fb|121|||entities|A  # per AA, pull all A entities data
        fb|123|C|10|entities|A  # per C 10, pull all A entities data

        # A job tasked with fetch of given day's per-hour breakdown
        fb|123|||dayhour|C|2018-02-03  # per AA 123, level C, breakdown by hour for date
        fb|123|C|10|dayhour||2018-02-03  # per C 10, breakdown by hour for date



    The "optionality" of given argument is expressed by omitting the value, but
    preserving its place in the fields order. However, trailing delimiters are stripped

    """
    base_parts = dict(
        ad_account_id=None,
        entity_type=None,
        entity_id=None,
        report_type=None,
        report_variant=None,
        range_start=None,
        range_end=None,
        namespace=NAMESPACE,
        component_vendor=config.application.UNIVERSAL_ID_COMPONENT_VENDOR,
        component_id=config.application.UNIVERSAL_ID_COMPONENT,
    )
    base_parts.update(parts)

    # per Universal ID spec, we must URL+Plus encode all parts
    # https://operam.atlassian.net/wiki/spaces/EN/pages/160596078/Universal+IDs

    parts = [
        _id_parts_converters.get(
            field,
            _id_parts_default_converter
        )(
            base_parts.get(field)
        )
        for field in fields
    ] + [
        _id_parts_default_converter(part)
        for part in trailing_parts or []
    ]

    return ID_DELIMITER.join([
        quote_plus(part)
        for part in parts
    ]).strip(ID_DELIMITER)


def generate_universal_id(
    fields=universal_id_fields,
    trailing_parts=None,
    **parts
):
    return generate_id(fields=fields, trailing_parts=trailing_parts, **parts)


_base_part_parser = lambda v: unquote_plus(v) if v else None
_default_part_parser = lambda v: v


_datetime_part_parser_input_len_formats_map = {
    len('####-##-##'): '%Y-%m-%d',
    len('####-##-##T##'): '%Y-%m-%dT%H',
    len('####-##-##T##:##'): '%Y-%m-%dT%H:%M',
    len('####-##-##T##:##:##'): '%Y-%m-%dT%H:%M:%S'
}


def _datetime_part_parser(v):
    # this is intentionally designed to pass through un-parse-able strings.
    if isinstance(v, str):
        # likely ISO-looking datetime string.
        format_string = _datetime_part_parser_input_len_formats_map.get(len(v))
        if format_string:
            try:
                if format_string == '%Y-%m-%d':
                    return datetime.strptime(v, format_string).date()
                else:
                    return datetime.strptime(v, format_string)
            except (ValueError, TypeError): # the rest should throw
                pass

    return v


_field_part_parsers_map = {
    'range_start': _datetime_part_parser,
    'range_end': _datetime_part_parser,
}


def parse_id(id_str, fields=fields):
    """
    This parser is for Job IDs - things that have prescribed number and order of parts

    Unviersal IDs are a bit different from Job IDs. Universal IDs
    have trailing positional data of undefined length.

    We dont' really need to parse Universal IDs in this project. Only form them.
    Generation of Universal IDs is part of formal API (above) and it (should be)
    properly tested.
    Parsing of Universal IDs is only needed in tests and whatever accommodation
    we make in this function to preserve and communicate the non-normative
    "trailing_parts" is there just for help in testing and because it's annoying
    to have a separate parse_id_but_with_trailing_stuff_just_for_testing() implementation.

    To parse Universal IDs with this, feed a specific list of fields to this function
    and hope for the best.

    :param id_str:
    :param list fields:
    :return:
    """

    id_parts = id_str.split(ID_DELIMITER)

    if len(fields) < len(id_parts):
        # this may change if we have field definitions that are clever enough to
        # consume more than one block of parts. There, we'll need to move this
        # check to the bottom to look at remainder
        raise ValueError(
            f'ID "{id_str}" contains {len(id_parts)} parts which is more '
            f'than the number of declared id components {fields}'
        )

    # intentionally allowing zip_longest to create None stand in values
    # for fields that are missing in id_parts
    # This way we treat fields list as the data contracts - these fields will
    # be present as keys with None values in the resulting data.
    return {
        field: _field_part_parsers_map.get(field, _default_part_parser)(_base_part_parser(part))
        for field, part in zip_longest(fields, id_parts)
    }


def parse_id_parts(job_id):
    # type: (str) -> JobIdParts
    """
    Very specific parser that returns a particular type - JobIdParts with
    very specific list of attributes. Used mostly for easing introspectation
    while working with parsed job_ids

    Relies on default list of `fields` that corresponds to frozen schema for job_id formation.

    :param job_id:
    :return:
    """
    return JobIdParts(**parse_id(job_id))
