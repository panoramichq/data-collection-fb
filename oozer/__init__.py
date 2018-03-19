# Ensure FB will get patched correctly
from common.facebook.patch import patch_facebook_sdk
from common.twitter.patch import patch_twitter_sdk

patch_facebook_sdk()
patch_twitter_sdk()
