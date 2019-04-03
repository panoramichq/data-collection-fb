# must be first, as it does event loop patching and other "first" things
from tests.base.testcase import TestCase

from oozer.common.facebook_fields import collapse_fields_children


class FacebookFieldsCollapserTests(TestCase):
    def test_do_nothing(self):

        should_be = ['a', 'b', 'c']

        assert collapse_fields_children(['a', 'b', 'c']) == should_be

    def test_dont_mess_with_existing_format(self):

        should_be = ['a', 'b{ba,bb}', 'c']

        assert collapse_fields_children(['a', 'b{ba,bb}', 'c']) == should_be

    def test_one_level_tuple(self):

        should_be = ['a', 'b{ba,bb}', 'c']

        assert collapse_fields_children(['a', ('b', ['ba', 'bb']), 'c']) == should_be

    def test_one_level_list(self):

        should_be = ['a', 'b{ba,bb}', 'c']

        assert collapse_fields_children(['a', ['b', ['ba', 'bb']], 'c']) == should_be

    def test_many_levels_tuple(self):

        should_be = ['a', 'b{ba,bb,bc{bca,bcb},bd}', 'c']

        assert collapse_fields_children(['a', ('b', ['ba', 'bb', ('bc', ['bca', 'bcb']), 'bd']), 'c']) == should_be

    def test_many_levels_list(self):

        should_be = ['a', 'b{ba,bb,bc{bca,bcb},bd}', 'c']

        assert collapse_fields_children(['a', ['b', ['ba', 'bb', ['bc', ['bca', 'bcb']], 'bd']], 'c']) == should_be
