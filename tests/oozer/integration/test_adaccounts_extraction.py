from tests.base.testcase import TestCase, integration

from requests.exceptions import HTTPError

from oozer.common.console_api import ConsoleApi
from config.operam_console_api import TOKEN


@integration('operam_console_api')
class TestingConsoleApiClient(TestCase):

    def test_get_active_adaccounts_with_valid_token(self):
        console_api = ConsoleApi(TOKEN, 'twitter')

        accounts = console_api.get_active_accounts()

        assert accounts

    def test_get_active_adaccounts_with_invalid_token(self):
        console_api = ConsoleApi('DUMMY_TOKEN', 'twitter')

        with self.assertRaises(HTTPError) as ex:
            console_api.get_active_accounts()

        assert '404' in str(ex.exception)
