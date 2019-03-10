from tests.base.testcase import TestCase, integration

from requests.exceptions import HTTPError

from oozer.common.console_api import ConsoleApi
from config.operam_console_api import TOKEN


@integration('operam_console_api')
class TestingConsoleApiClient(TestCase):

    def test_get_active_adaccounts_with_valid_token(self):
        accounts = ConsoleApi.get_accounts(TOKEN, active=True)

        assert accounts

    def test_get_active_adaccounts_with_invalid_token(self):
        with self.assertRaises(HTTPError) as ex:
            ConsoleApi.get_accounts('INVALID', active=True)

        assert '404' in str(ex.exception)
