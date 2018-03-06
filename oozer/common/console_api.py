import requests
from config.operam_console_api import TOKEN, URL

class ConsoleApi:
    token = None  # type: str

    def __init__(self, token):

        self.token = token

    def get_active_accounts(self):
        response = requests.get(
            f'{URL}/api/projects/platform-accounts',
            headers={'x-auth-token': TOKEN},
            params= {'platform': 'facebook'}
        )

        return response.json()

