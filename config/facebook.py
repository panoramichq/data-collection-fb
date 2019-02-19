
# used only for local dev
# running bin/migrate.py with these inserts the objects
AD_ACCOUNT = None
AD_ACCOUNT_TIME_ZONE = 'America/Los_Angeles'
TOKEN = 'bogus token'

# Real Ad Accounts, tokens are injected by collection workers

# Polling intervals for Facebook insights jobs
INSIGHTS_STARTING_POLLING_INTERVAL = 0.5
INSIGHTS_POLLING_INTERVAL = 0.5


from common.updatefromenv import update_from_env
update_from_env(__name__)
