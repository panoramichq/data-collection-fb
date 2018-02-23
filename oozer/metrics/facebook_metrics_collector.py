import gevent
from facebookads.exceptions import FacebookRequestError

from config.facebook import INSIGHTS_POLLING_INTERVAL, \
    INSIGHTS_STARTING_POLLING_INTERVAL

from common.enums.failure_bucket import FailureBucket
from oozer.common.report_job_status import JobStatus
from facebookads.adobjects.adreportrun import AdReportRun
from oozer.common.facebook_async_report import FacebookAsyncReportStatus
from oozer.common.job_scope import JobScope
from oozer.common import cold_storage
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.facebook_api import FacebookApiContext, get_default_fields


def _execute_report(fb_entity, report_params, job_scope):
    """
    Run the actual execution of the insights job

    :param fb_entity: The ads api facebook entity instance
    :param dict report_params: FB API report params
    :param JobScope job_scope: The JobScope as we get it from the task itself
    """
    report_status_obj = fb_entity.get_insights(
        params=report_params,
        async=True
    )  # type: AdReportRun

    report_tracker = FacebookAsyncReportStatus(report_status_obj)

    polling_interval = INSIGHTS_STARTING_POLLING_INTERVAL
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
        polling_interval = INSIGHTS_POLLING_INTERVAL

    return report_tracker.iter_report_data()


class FacebookInsightsJobStatus(JobStatus):
    """
    Use this to communicate to give status reporter enough information to
    figure out what the stage id means in terms of failures
    """

    # Progress states
    Start = 100
    InColdStore = 500

    # Various error states
    TooMuchData = (-500, FailureBucket.TooLarge)
    ThrottlingError = (-700, FailureBucket.Throttling)
    GenericFacebookError = -900
    GenericError = -1000


def collect_insights(fb_entity_edge, report_params, job_scope, job_context):
    """
    The actual insights collection routine

    :param fb_entity_edge:
    :param FacebookReportDefinition report_params:
    :param job_scope:
    :param job_context:
    :return:
    """

    # TODO: Augment report_params from job scope

    # Report start of work
    report_job_status_task.delay(FacebookInsightsJobStatus.Start, job_scope)

    try:
        with FacebookApiContext(job_scope.token) as context:

            FbModel = context.to_fb_model(job_scope.entity_id, fb_entity_edge)

            job_scope_base_data = job_scope.to_dict()

            # Iterate through the insights
            for item in _execute_report(FbModel, report_params.to_dict(), job_scope):

                # TODO: Add status job reporting, esp. normative reporting has
                # to be divined from the job scope
                normative_job_scope = JobScope(
                    job_scope_base_data,
                #     entity_id=item.get('id'),
                #     entity_type=
                )

                # Store the individual datum, use job context for the cold
                # storage thing to divine whatever it needs from the job context
                cold_storage.store(item, normative_job_scope)

                yield item

            # Report on the effective task status
            report_job_status_task.delay(
                FacebookInsightsJobStatus.InColdStore, job_scope
            )

    except FacebookRequestError as e:
        # Check for this
        # error_code = 100,  CodeException (error subcode: 1487534)
        # ^ means we asked for too much data

        # Inspect the exception for FB exceptions, so we can understand what's
        # going on
        report_job_status_task.delay(
            FacebookInsightsJobStatus.GenericFacebookError, job_scope,
        )
        raise

    except Exception:
        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            FacebookInsightsJobStatus.GenericError, job_scope
        )
        raise

