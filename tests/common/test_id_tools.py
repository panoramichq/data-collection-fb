# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from common import id_tools
from datetime import datetime, date
from tests.base import random
from urllib.parse import quote_plus

NS = id_tools.NAMESPACE
D = id_tools.ID_DELIMITER


class TestingIdGenerationTools(TestCase):
    def test_it_works_with_nulls(self):
        assert NS == id_tools.generate_id()

    def test_universal_works_with_nulls(self):
        assert D.join(['oprm', 'm', NS]) == id_tools.generate_universal_id()

    def test_some_data_at_end(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        id_should_be = D.join(
            [
                # 'oprm',
                # 'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                # '', # report variant
                # '', # Range start
                # '', # Range end
            ]
        )

        assert id_should_be == id_tools.generate_id(ad_account_id=ad_account_id, report_type=report_type)

    def test_some_data_trailing(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        id_should_be = D.join(
            [
                # 'oprm',
                # 'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                '',  # Range start
                '',  # Range end
                'hocus',
                '',  # None
                'pocus'
            ]
        )

        assert id_should_be == id_tools.generate_id(
            ad_account_id=ad_account_id, report_type=report_type, trailing_parts=['hocus', None, 'pocus']
        )

    def test_some_data_trailing_universal(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        id_should_be = D.join(
            [
                'oprm',
                'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                '',  # Range start
                '',  # Range end
                'hocus',
                '',  # None
                'pocus'
            ]
        )

        assert id_should_be == id_tools.generate_universal_id(
            ad_account_id=ad_account_id, report_type=report_type, trailing_parts=['hocus', None, 'pocus']
        )

    def test_date_handling(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        range_start_d = date.today()
        range_start_should_be = range_start_d.strftime('%Y-%m-%d')

        id_should_be = D.join(
            [
                'oprm',
                'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                range_start_should_be,  # Range start
                # '', # Range end
            ]
        )

        assert id_should_be == id_tools.generate_universal_id(
            ad_account_id=ad_account_id, report_type=report_type, range_start=range_start_d
        )

    def test_datetime_handling(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        range_start_dt = datetime(2000, 1, 31, 3)
        range_start_should_be = range_start_dt.strftime('%Y-%m-%dT%H')

        id_should_be = D.join(
            [
                'oprm',
                'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                range_start_should_be,  # Range start
                # '', # Range end
            ]
        )

        assert id_should_be == id_tools.generate_universal_id(
            ad_account_id=ad_account_id, report_type=report_type, range_start=range_start_dt
        )

    def test_datetime_handling_special_zero_hour_handling(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        range_start_dt = datetime(2000, 1, 31, 0, 0, 0)
        # even though id-forming logic shortens the string to last
        # non-zero value, this does not apply to hours.
        # Whenever value is DateTime type, hours are always
        # part of string even when hour is zero.
        range_start_should_be = range_start_dt.strftime('%Y-%m-%dT%H')

        id_should_be = D.join(
            [
                'oprm',
                'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                range_start_should_be,  # Range start
                # '', # Range end
            ]
        )

        assert id_should_be == id_tools.generate_universal_id(
            ad_account_id=ad_account_id, report_type=report_type, range_start=range_start_dt
        )

    def test_datetime_handling_and_minutes(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        range_start_dt = datetime(2000, 1, 31, 3, 59)
        range_start_should_be = quote_plus(range_start_dt.strftime('%Y-%m-%dT%H:%M'))

        id_should_be = D.join(
            [
                'oprm',
                'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                range_start_should_be,  # Range start
                # '', # Range end
            ]
        )

        assert id_should_be == id_tools.generate_universal_id(
            ad_account_id=ad_account_id, report_type=report_type, range_start=range_start_dt
        )

    def test_datetime_handling_and_seconds(self):
        ad_account_id = random.gen_string_id()
        report_type = 'blah'

        range_start_dt = datetime(2000, 1, 31, 3, 0, 47)
        range_start_should_be = quote_plus(range_start_dt.strftime('%Y-%m-%dT%H:%M:%S'))

        id_should_be = D.join(
            [
                'oprm',
                'm',
                NS,
                ad_account_id,
                '',  # entity Type
                '',  # entity ID
                report_type,
                '',  # report variant
                range_start_should_be,  # Range start
                # '', # Range end
            ]
        )

        assert id_should_be == id_tools.generate_universal_id(
            ad_account_id=ad_account_id, report_type=report_type, range_start=range_start_dt
        )


class TestingIdParsingTools(TestCase):
    def test_fields_custom_short(self):

        with self.assertRaises(ValueError) as ex_catcher:
            id_tools.parse_id(D.join(['a', 'b']), fields=['one_only'])

        assert 'contains 2 parts which is more than the number of declared id components' in str(ex_catcher.exception)

    def test_fields_custom_exact(self):

        id_data_actual = id_tools.parse_id(D.join(['A', 'B']), fields=['a', 'b'])

        id_data_should_be = dict(a='A', b='B')

        assert id_data_actual == id_data_should_be

    def test_fields_custom_extra(self):

        id_data_actual = id_tools.parse_id(D.join(['A', 'B']), fields=['a', 'b', 'c'])

        id_data_should_be = dict(a='A', b='B', c=None)

        assert id_data_actual == id_data_should_be

    def test_value_url_decoding(self):

        id_data_actual = id_tools.parse_id(D.join(['A+A', 'B%20B']), fields=['a', 'b', 'c'])

        id_data_should_be = dict(a='A A', b='B B', c=None)

        assert id_data_actual == id_data_should_be

    def test_fields_default(self):

        id_data_actual = id_tools.parse_id(D.join([NS, '123'])
                                           # fields=['a', 'b', 'c']
                                           )

        id_data_should_be = dict(
            namespace=NS,
            ad_account_id='123',
            entity_id=None,
            entity_type=None,
            range_end=None,
            range_start=None,
            report_type=None,
            report_variant=None
        )

        assert id_data_actual == id_data_should_be

    def test_fields_default_universal(self):

        id_data_actual = id_tools.parse_id(D.join(['oprm', 'm', NS, '123']), fields=id_tools.universal_id_fields)

        id_data_should_be = dict(
            component_vendor='oprm',
            component_id='m',
            namespace=NS,
            ad_account_id='123',
            entity_id=None,
            entity_type=None,
            range_end=None,
            range_start=None,
            report_type=None,
            report_variant=None,
        )

        assert id_data_actual == id_data_should_be


class TestingIdParsingDatetimeFieldsTools(TestCase):
    def test_datetime_parsing_date_type(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '2010-01-01']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: date(2010, 1, 1)}

            assert id_data_actual == id_data_should_be

    def test_datetime_parsing_datetime_type_for_zeros(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '2010-01-01T00']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: datetime(2010, 1, 1, 0, 0, 0)}

            assert id_data_actual == id_data_should_be, 'Must not be downgraded to date type, even with zero hours'

    def test_datetime_parsing_datetime_type(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '2010-01-02T03']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: datetime(2010, 1, 2, 3, 0, 0)}

            assert id_data_actual == id_data_should_be

    def test_datetime_parsing_datetime_type_minutes(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '2010-01-02T03:04']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: datetime(2010, 1, 2, 3, 4, 0)}

            assert id_data_actual == id_data_should_be

    def test_datetime_parsing_datetime_type_seconds(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '2010-01-02T03:04:05']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: datetime(2010, 1, 2, 3, 4, 5)}

            assert id_data_actual == id_data_should_be

    def test_datetime_parsing_goobledygook_passthrough(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '1234567890T03E04E05']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: '1234567890T03E04E05'}

            assert id_data_actual == id_data_should_be

    def test_datetime_parsing_none_passthrough(self):

        datetime_fields = ['range_start', 'range_end']

        for field in datetime_fields:
            id_data_actual = id_tools.parse_id(D.join(['A', '']), fields=['a', field])

            id_data_should_be = {'a': 'A', field: None}

            assert id_data_actual == id_data_should_be
