ENVIRONMENT = 'testing'
"""
Environment the application runs in
"""

NAME = 'data-collection-fb'
"""
All custom metrics will be prefixed with the value (and dot at the end)
"""

from common.updatefromenv import update_from_env
update_from_env(__name__)
