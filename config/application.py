# flake8: noqa: E722

ENVIRONMENT = 'dev'
"""
Environment the application runs in
"""

NAME = 'data-collection-fb'
"""
All custom metrics will be prefixed with the value (and dot at the end)
"""

UNIVERSAL_ID_COMPONENT = 'm'
UNIVERSAL_ID_SYSTEM_NAMESPACE = 'o'  # stands for 'operam'
UNIVERSAL_ID_COMPONENT_VENDOR = 'oprm'
"""
This is a coordinated effort to use same univeral IDs "internal" namespace
for deterministic generation of compound object (and job) IDs

See https://operam.atlassian.net/wiki/spaces/EN/pages/160596078/Universal+IDs
"""

PERMANENTLY_FAILING_JOB_THRESHOLD = 10
RECOLLECT_OLDER_THAN = None

from common.updatefromenv import update_from_env

update_from_env(__name__)
