
# used only for local dev
# running bin/migrate.py with these inserts the objects
AD_ACCOUNT = None
AD_ACCOUNT_TIME_ZONE = 'America/Los_Angeles'
TOKEN = 'bogus token'

# Real Ad Accounts, tokens are injected by collection workers

from common.updatefromenv import update_from_env
update_from_env(__name__)
