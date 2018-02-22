import gevent
import time

from typing import Generator, Dict

from facebookads.adobjects.adreportrun import AdReportRun
from facebookads.adobjects.adsinsights import AdsInsights

from oozer.common.facebook_api import FacebookApiContext
from oozer.common.facebook_async_report import FacebookAsyncReportStatus
from oozer.common.enum import to_fb_model
from common.enums.entity import Entity
from oozer.common.job_scope import JobScope


STARTING_POLLING_INTERVAL = 0.1
POLLING_INTERVAL = 1


def get_insights(fb_entity, report_params):
    # type: (Any) -> Generator[Dict]
    """
    A wrapper for Async Insights report data fetching.
    (At this time) blocks while polling on report and returns
    a generator that yields clean dicts - one per element in
    returned report.

    :param fb_entity: one of FB Ads SDK models for AA, C, AS, A
    :param dict report_params:
    :return: Generator yielding individual records from the report
    :rtype: Generator[Dict]
    """

    report_status_obj = fb_entity.get_insights(
        params=report_params,
        async=True
    )  # type: AdReportRun

    report_tracker = FacebookAsyncReportStatus(report_status_obj)

    polling_interval = STARTING_POLLING_INTERVAL
    while not report_tracker.is_complete:
        # My prior history of interaction with FB's API for async reports
        # tells me they need a little bit of time to bake the report status record fully
        # Asking for its status immediately very very often provided bogus results:
        # - "Failed" while it's just being constructed, then switching to "pending" then to final state
        # - "Successful" while it's just being constructed, then switching to "pending" then to final state
        # So sleeping a little before asking for it first time is better then sleeping
        # AFTER asking for status the first time. Sleep first.
        # TODO: change this to Gevent sleep or change whole thing into a generator that
        # yields nothing until it raises exception for failure or returns Generator with data.
        gevent.sleep(polling_interval)
        report_tracker.refresh()
        polling_interval = POLLING_INTERVAL

    return report_tracker.iter_report_data()


def _generate_report_args(job_scope):
    """
    :param JobScope job_scope: Value one would get from job_scope.to_dict()
    :rtype: dict
    """
    # by parent vs not
    # root object type
    # level object type



def collect_insights(job_scope):
    """
    :param JobScope job_scope: The JobScope as we get it from the task itself
    """

    fb_entity, report_params = _generate_report_args(job_scope)
