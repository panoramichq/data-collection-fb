# used only for local dev
# running bin/migrate.py with these inserts the objects
AD_ACCOUNT = None
AD_ACCOUNT_TIME_ZONE = 'America/Los_Angeles'

# The auth is against an app (app receives permissions to other promotable entities)
# For now this is just one - not sure if balancing the usage between several apps makes sense at the moment
# but we are going to implement this so we're prepared for such situation (similarly to FB).
CONSUMER_KEY = None
CONSUMER_SECRET = None
TOKEN = None
SECRET = 'bogus token'

# Polling intervals for Twitter insights jobs
INSIGHTS_STARTING_POLLING_INTERVAL = 0.1
INSIGHTS_POLLING_INTERVAL = 0.1


from common.updatefromenv import update_from_env
update_from_env(__name__)
