# flake8: noqa: E722

TOKEN = None
URL = 'https://console.operam.com'

from common.updatefromenv import update_from_env
update_from_env(__name__)
