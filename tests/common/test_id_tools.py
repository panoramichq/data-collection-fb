# must be first, as it does event loop patching and other "first" things
from common.id_tools import generate_id
from tests.base.testcase import TestCase

class TestingIdTools(TestCase):

    def test_required_namespace(self):
        with self.assertRaises(AssertionError) as ex:
            generate_id(namespace=None)

        assert 'Namespace is a required parameter' in str(ex.exception)

    def test_it_works_with_nulls(self):

        generate_id()
