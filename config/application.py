ENVIRONMENT = 'testing'
"""
Environment the application runs in
"""

NAME = 'data-collection-fb'
"""
All custom metrics will be prefixed with the value (and dot at the end)
"""

UNIVERSAL_ID_SYSTEM_NAMESPACE = 'o'  # stands for 'operam'
"""
This is a coordinated effort to use same univeral IDs "internal" namespace
for deterministic generation of compound object (and job) IDs

See https://operam.atlassian.net/wiki/spaces/EN/pages/160596078/Universal+IDs
"""

from common.updatefromenv import update_from_env
update_from_env(__name__)
