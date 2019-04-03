# flake8: noqa: E722

API_KEY = None
"""
The API key corresponding to our project
"""

from common.updatefromenv import update_from_env

update_from_env(__name__)
