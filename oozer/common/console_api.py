import requests
from config.operam_console_api import URL


class ConsoleApi:
    token = None  # type: str

    def __init__(self, token, platform):

        self.token = token
        self.platform = platform

    def get_active_accounts(self):
        response = requests.get(
            f'{URL}/api/projects/platform-accounts',
            headers={'x-auth-token': self.token},
            params= {'platform': self.platform}
        )

        # Raise for 4xx and 5xx codes for now
        response.raise_for_status()
        return response.json()

