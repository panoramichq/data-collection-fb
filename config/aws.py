# flake8: noqa: E722

S3_ENDPOINT = None
"""
If you need to manually set s3 endpoint, this is the place. This is especially
useful for testing
"""

S3_BUCKET_NAME = 'operam-metrics-reports'
S3_BUCKET_RAW_NAME = 'operam-metrics-reports-raw'

"""
The bucket name where we push our report files
"""

from common.updatefromenv import update_from_env

update_from_env(__name__)
