from typing import Any

import requests
from config.operam_console_api import URL


class ConsoleApi:

    @staticmethod
    def _fetch_platform_type_from_api(platform_type: str, token: str, params: Any):
        response = requests.get(
            f'{URL}/api/projects/platform-accounts',
            headers={'x-auth-token': token},
            params={'platform': platform_type, **params}
        )

        # Raise for 4xx and 5xx codes for now
        response.raise_for_status()
        return response.json()

    @staticmethod
    def get_accounts(token: str, **params: Any):
        return ConsoleApi._fetch_platform_type_from_api('facebook', token, params)

    @staticmethod
    def get_pages(token: str, **params: Any):
        return ConsoleApi._fetch_platform_type_from_api('facebook_page', token, params)
