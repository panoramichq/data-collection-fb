import requests
from config.operam_console_api import URL


class ConsoleApi:
    token = None  # type: str

    def __init__(self, token):

        self.token = token

    def get_accounts(self, **params):
        response = requests.get(
            f'{URL}/api/projects/platform-accounts',
            headers={'x-auth-token': self.token},
            params= {'platform': 'facebook', **params}
        )

        # Raise for 4xx and 5xx codes for now
        response.raise_for_status()
        return response.json()

