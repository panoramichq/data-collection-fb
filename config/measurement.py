ENABLED = False
"""
Setting this to false will disable the metrics reporting altogether
"""

STATSD_SERVER = 'dd-agent'
"""
The address of the statsd server you will be sending measurements to
"""

STATSD_PORT = 8125
"""
The port of the statsd server
"""

METRIC_PREFIX = 'data-collection-fb'
"""
All custom metrics will be prefixed with the value (and dot at the end)
"""

PREFIX_COUNTER = 'counters'
PREFIX_TIMING = 'timers'
PREFIX_GAUGE = 'gauges'
PREFIX_SET = 'sets'
"""
Prefixes for individual metric types
"""