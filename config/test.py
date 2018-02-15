# Facebook Ad Account we use for testing (Operam Engineering test)
FB_ADACCOUNT_ID = '2034428216844013'

# Access token to use when calling Facebook marketing API
FB_ACCESS_TOKEN = None

from common.updatefromenv import update_from_env
update_from_env(__name__)
