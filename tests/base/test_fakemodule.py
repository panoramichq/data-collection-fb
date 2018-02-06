import sys

from unittest import TestCase

from .fakemodule import FakeModule


class TestingFakeModule(TestCase):

    def test_create_and_clean(self):

        with FakeModule('a.b.c') as m:
            assert sys.modules['a']
            assert sys.modules['a.b']
            assert sys.modules['a.b.c']

        assert 'a.b.c' not in sys.modules

    def test_body_applied(self):

        with FakeModule('a.b.c', 'x = 1') as m:
            assert m.x == 1