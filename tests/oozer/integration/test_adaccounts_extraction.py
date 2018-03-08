from datetime import datetime

from requests.exceptions import HTTPError

from oozer.common.console_api import ConsoleApi
from tests.base.testcase import TestCase, integration

from config.operam_console_api import TOKEN

# FIXME: check a console_integration decorator or change logic in integration
# @integration
class TestingAdaccountsExtraction(TestCase):
    def test_get_active_adaccounts_with_valid_token(self):
        console_api = ConsoleApi(TOKEN)

        accounts = console_api.get_active_accounts()

        assert accounts

    def test_get_active_adaccounts_with_invalid_token(self):
        console_api = ConsoleApi()

        with self.assertRaises(HTTPError) as ex:
            print(str(ex))
            console_api.get_active_accounts()
