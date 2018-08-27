import os

# botocore has a problem. Does not like reading keys from env var
# Only file-based credentials are picked up, but that is very
# annoying to manage when we are trying to inject that from outside
# the container. So, brute-forcing this for local testing convenience.
# In prod / stage, if these are not set, botocore will pick up
# stuff from the Role set on container.
# Using this to hit prod / stage dynamo tables from local for data validation.
ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID') or None
SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY') or None

S3_ENDPOINT = None
"""
If you need to manually set s3 endpoint, this is the place. This is especially
useful for testing
"""

S3_BUCKET_NAME = 'operam-metrics-reports'
"""
The bucket name where we push our report files
"""

from common.updatefromenv import update_from_env
update_from_env(__name__)
