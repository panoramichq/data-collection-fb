import json
import requests
from config.operam_scraper_api import URL


class ScraperApi:
    token = None  # type: str

    def __init__(self, token):

        self.token = token

    def get_pages(self, **params):
        response = requests.get(
            f'{URL}/v1/sources',
            headers={'x-api-key': self.token},
            params= {'filter': json.dumps({'name': 'kind',
                                           'op': 'any',
                                           'val': ['facebook_page']}),
                     **params
                }
        )

        # Raise for 4xx and 5xx codes for now
        response.raise_for_status()
        return response.json()

