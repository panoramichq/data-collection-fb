# flake8: noqa: E722

BUILD_ID = 'latest'
COMMIT_ID = None

from common.updatefromenv import update_from_env
update_from_env(__name__)
