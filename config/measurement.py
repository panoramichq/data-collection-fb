# flake8: noqa: E722

STATSD_SERVER = 'localhost'
"""
The address of the statsd server you will be sending measurements to
"""

STATSD_PORT = 8125
"""
The port of the statsd server
"""

SOCKET_PATH = None

METRIC_PREFIX = 'data-collection-fb'
"""
All custom metrics will be prefixed with the value (and dot at the end)
"""

PREFIX_COUNTER = 'counters'
PREFIX_GAUGE = 'gauges'
PREFIX_HISTOGRAM = 'histogram'
PREFIX_SET = 'sets'
PREFIX_TIMING = 'timers'
"""
Prefixes for individual metric types
"""

from common.updatefromenv import update_from_env

update_from_env(__name__)
