import gevent

from datetime import datetime, date
from facebookads.exceptions import FacebookError
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.adobjects.abstractcrudobject import AbstractCrudObject
from typing import Callable, Dict

from config.facebook import INSIGHTS_POLLING_INTERVAL, \
    INSIGHTS_STARTING_POLLING_INTERVAL

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.enums.reporttype import ReportType
from facebookads.adobjects.adreportrun import AdReportRun
from oozer.common import cold_storage
from oozer.common.facebook_api import (
    FacebookApiContext,
    FacebookApiErrorInspector
)
from oozer.common.facebook_async_report import FacebookAsyncReportStatus
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status import FacebookJobStatus
from oozer.common.report_job_status_task import report_job_status_task


ENUM_LEVEL_MAP = {
    Entity.AdAccount: AdsInsights.Level.account,
    Entity.Campaign: AdsInsights.Level.campaign,
    Entity.AdSet: AdsInsights.Level.adset,
    Entity.Ad: AdsInsights.Level.ad,
}


REPORT_TYPE_FB_BREAKDOWN_ENUM = {
    ReportType.day_age_gender: [ AdsInsights.Breakdowns.age, AdsInsights.Breakdowns.gender ],
    ReportType.day_dma: [ AdsInsights.Breakdowns.dma ],
    ReportType.day_hour: [ AdsInsights.Breakdowns.hourly_stats_aggregated_by_advertiser_time_zone ],
}


DEFAULT_REPORT_FIELDS = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.ad_id,

    # Non-unique

    # Essential
    AdsInsights.Field.spend,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.actions,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p95_watched_actions,
    AdsInsights.Field.video_p100_watched_actions,
    AdsInsights.Field.video_10_sec_watched_actions,
    AdsInsights.Field.video_30_sec_watched_actions,

    # Good to have
    AdsInsights.Field.cost_per_action_type,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cpp,
    AdsInsights.Field.ctr,
    AdsInsights.Field.cpc,
    AdsInsights.Field.relevance_score,

    AdsInsights.Field.video_avg_time_watched_actions,
    AdsInsights.Field.video_avg_percent_watched_actions,

    # Not sure
    # AdsInsights.Field.action_values,
    # 'inline_link_clicks',
    # 'inline_post_engagement',
    # 'social_clicks',
    # 'social_impressions',
    # 'social_reach',
    # 'social_spend',
    #
    # 'action_values',
    # 'buying_type',
    # 'call_to_action_clicks',
    # 'cost_per_10_sec_video_view',
    # 'cost_per_estimated_ad_recallers',
    # 'cost_per_inline_link_click',
    # 'cost_per_inline_post_engagement',
    # 'cost_per_total_action',
    # 'estimated_ad_recall_rate',
    # 'estimated_ad_recallers',
    # 'total_action_value',
    # 'video_10_sec_watched_actions',
    # 'video_30_sec_watched_actions',
    # 'website_ctr',


    # Unique

    # Essential
    AdsInsights.Field.unique_actions,
    AdsInsights.Field.reach,

    # Good to have
    AdsInsights.Field.frequency,
    AdsInsights.Field.cost_per_unique_action_type,

    # Not sure
    # 'cost_per_unique_click',
    # 'total_unique_actions',
    #
    # 'unique_clicks',
    # 'unique_ctr',
    # 'unique_link_clicks_ctr',
    # 'unique_social_clicks',
]

DEFAULT_ATTRIBUTION_WINDOWS = [
    AdsInsights.ActionAttributionWindows.value_1d_view,
    AdsInsights.ActionAttributionWindows.value_7d_view,
    AdsInsights.ActionAttributionWindows.value_28d_view,
    AdsInsights.ActionAttributionWindows.value_1d_click,
    AdsInsights.ActionAttributionWindows.value_7d_click,
    AdsInsights.ActionAttributionWindows.value_28d_click,
]


def iter_insights(fb_entity, report_params):
    """
    Run the actual execution of the insights job

    :param AbstractCrudObject fb_entity: The ads api facebook entity instance
    :param dict report_params: FB API report params
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


def _convert_and_validate_date_format(dt):
    """
    Converts incoming values that may represent a date
    into a FB-specific stringified date format
    that is acceptable for the `time_range` report parameter

    :param Union[str,datetime,date] dt:
    :return: A string of format 'YYYY-MM-DD'
    """

    # datetime is actually a subclass of date class
    # but for clarity of what we are doing, will check against
    # both, though only comparing to date is needed
    if not isinstance(dt, (date, datetime)):
        # we assume it's some string
        try:
            dt = datetime.strptime(dt, '%Y-%m-%d')
        except (ValueError, TypeError):
            raise ValueError(
                f"Value '{dt}' cannot be read as 'YYYY-MM-DD' string"
            )
    return dt.strftime('%Y-%m-%d')


class BaseStoreHandler:

    def __init__(self, job_scope):
        self.job_scope = job_scope

    def store(self, datum):
        pass

    def __enter__(self):
        return self.store

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class NormalStore(BaseStoreHandler):

    def store(self, datum):
        cold_storage.store(datum, self.job_scope)


class MemorySpoolStore(BaseStoreHandler):

    def __init__(self, job_scope):
        super().__init__(job_scope)
        self.data = []

    def store(self, datum):
        self.data.append(datum)

    def __exit__(self, exc_type, exc_val, exc_tb):
        cold_storage.store(self.data, self.job_scope)


class ChunkDumpStore(BaseStoreHandler):

    def __init__(self, job_scope, chunk_size=50):
        super().__init__(job_scope)
        self.data = []
        self.chunk_marker = 0
        self.chunk_size = chunk_size

    def store(self, datum):
        self.data.append(datum)
        if len(self.data) == self.chunk_size:
            cold_storage.store(self.data, self.job_scope, self.chunk_marker)
            self.chunk_marker += 1
            self.data.clear()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self.data):
            cold_storage.store(self.data, self.job_scope, self.chunk_marker)


class NaturallyNormativeChildStore(BaseStoreHandler):

    def __init__(self, job_scope):
        super().__init__(job_scope)

        normative_entity_type = job_scope.report_variant
        assert normative_entity_type in Entity.ALL

        self.job_scope_base_data = job_scope.to_dict()
        # since we are converting per-parent into per-child
        # job signature, report_variant cannot be set
        self.job_scope_base_data.update(
            entity_type=normative_entity_type,
            is_derivative=True, # this keeps the scope from being counted as done task by looper
            report_variant=None,
        )
        self.id_attribute_name = {
            Entity.AdAccount: AdsInsights.Field.account_id,
            Entity.Campaign: AdsInsights.Field.campaign_id,
            Entity.AdSet: AdsInsights.Field.adset_id,
            Entity.Ad: AdsInsights.Field.ad_id
        }[normative_entity_type]

    def store(self, datum):
        entity_id = datum.get(self.id_attribute_name) or datum.get('id')
        assert entity_id, "This code must have an entity ID for building of unique insertion ID"
        normative_job_scope = JobScope(
            self.job_scope_base_data,
            entity_id=entity_id
        )
        # and store data under that per-entity, normative JobScope.
        cold_storage.store(datum, normative_job_scope)
        # since we report for many entities in this code,
        # must also communicate out the status inside of the for-loop
        # at the normative level.
        report_job_status_task.delay(
            FacebookJobStatus.Done, normative_job_scope
        )


class JobScopeParsed:

    report_params = None  # type: dict
    DatumHandler = None  # type: Callable[JobScope, Callable[Dict, None]]
    report_root_fb_entity = None  # type: AbstractCrudObject

    def __init__(self, job_scope):
        """
        :param JobScope job_scope:
        """

        if job_scope.report_type not in ReportType.ALL_METRICS:
            raise ValueError(
                f"Report type {job_scope.report_type} specified is not one of supported values: {ReportType.ALL_METRICS}"
            )
        # cool. we are in the right place...

        self.report_params = {
            'fields': DEFAULT_REPORT_FIELDS
        }

        # Next is (a) vs (b) - abstraction level determination
        is_per_parent_report = not job_scope.entity_id and job_scope.report_variant in Entity.ALL

        if is_per_parent_report:
            entity_id = job_scope.ad_account_id
            entity_type = Entity.AdAccount
            self.report_params.update(
                level=ENUM_LEVEL_MAP[job_scope.report_variant]
            )
        else: # direct, per-entity report
            entity_id = job_scope.entity_id
            entity_type = job_scope.entity_type
            self.report_params.update(
                level=ENUM_LEVEL_MAP[entity_type]
            )

        # Now, (c), (d), (e), (f), (g) choices
        # we already checked above that this is one of metrics report types
        # So we know it will be either lifetime or day-with-breakdown type
        # TODO: add fields listings appropriate for each type
        if job_scope.report_type == ReportType.lifetime:
            self.report_params.update(
                date_preset=AdsInsights.DatePreset.lifetime
            )
        else:  # some day-with-breakdown type
            self.report_params.update(
                time_increment=1,  # group by calendar day (in AA tz)
                time_range={
                    'since': _convert_and_validate_date_format(job_scope.range_start),
                    # No value for job_scope.range_end means 1-day report for range_start day
                    'until': _convert_and_validate_date_format(job_scope.range_end or job_scope.range_start),
                },
                breakdowns=REPORT_TYPE_FB_BREAKDOWN_ENUM[job_scope.report_type]
            )

        # Indicates that datum returned in a per-parent report is by itself
        # naturally mapped to some single normative job ,
        # meaning each element can be stored separately
        # but only under normative ID computed on the fly
        # from the datum.
        # This must be accompanied by a transform fn that
        # derives a normative ID from data.
        is_naturally_normative_child = (
            job_scope.report_type == ReportType.lifetime
        )

        # special case.
        # when report type is per-specific-single-entity-ID
        # AND one of per-day-with-breakdown
        # per-Entity-ID-per-day bundle with 24 records before saving it.
        # This results in a single write to the cold store under
        # single normative ID.
        is_whole_report_bundle_write = (
            # must be one of those per-day reports
            job_scope.report_type in ReportType.ALL_DAY_BREAKDOWNS
            # and the report is per single entity ID
            and job_scope.entity_id and not job_scope.report_variant
            # and report is for a single calendar day
            and self.report_params['time_range']['since'] == self.report_params['time_range']['until']
        )

        # a more complex variont of whole_report_bundle_write
        # where, while we canNOT spool entire report into memory to
        # write it as one bundle, we cannot really write each
        # individual result out either, as there will be a shit-load of them
        # and we have to write is some sort of batching mode, but
        # cannot cleanly group the bundles into per-normative-ID bundles,
        # and instead will write under effective ID, but with a suffix
        # indicating the monotonically-increasing chunk number.
        is_chunk_write = (
            # must be one of those crap-load of records per-day reports
            job_scope.report_type in ReportType.ALL_DAY_BREAKDOWNS
            # but not already covered by clean whole_report_bundle_write bundling
            # which means it can be a lot of records per report
            and not is_whole_report_bundle_write
        )

        if is_naturally_normative_child:
            self.DatumHandler = NaturallyNormativeChildStore
        elif is_whole_report_bundle_write:
            self.DatumHandler = MemorySpoolStore
        elif is_chunk_write:
            self.DatumHandler = ChunkDumpStore
        else:
            self.DatumHandler = NormalStore

        with FacebookApiContext(job_scope.token) as fb_ctx:
            self.report_root_fb_entity = fb_ctx.to_fb_model(
                entity_id, entity_type
            )


def iter_collect_insights(job_scope, job_context):
    """
    Central, *GENERIC* implementation of insights fetcher task

    The goal of this method is to be the entry point for
    metrics fetching Celery tasks. This method is expected to parse
    the JobScope object, figure out that needs to be done
    based on data in the JobScope object and convert that data into
    proper parameters for calling FB

    :param JobScope job_scope: The JobScope as we get it from the task itself
    :param JobContext job_context: A job context we use for entity checksums
    :rtype: Generator[Dict]
    """
    # Report start of work
    report_job_status_task.delay(FacebookJobStatus.Start, job_scope)

    try:
        scope_parsed = JobScopeParsed(job_scope)
        data_iter = iter_insights(
            scope_parsed.report_root_fb_entity,
            scope_parsed.report_params
        )
        with scope_parsed.DatumHandler(job_scope) as handle:
            cnt = 0
            for datum in data_iter:
                handle(datum)
                yield datum
                cnt += 1

                if cnt % 100 == 0:
                    report_job_status_task.delay(
                        FacebookJobStatus.DataFetched, job_scope
                    )

        report_job_status_task.delay(
            FacebookJobStatus.Done, job_scope
        )

    except FacebookError as e:
        # Build ourselves the error inspector
        inspector = FacebookApiErrorInspector(e)

        # Is this a throttling error?
        if inspector.is_throttling_exception():
            report_job_status_task.delay(
                FacebookJobStatus.ThrottlingError, job_scope
            )

        # Did we ask for too much data?
        elif inspector.is_too_large_data_exception():
            report_job_status_task.delay(
                FacebookJobStatus.TooMuchData, job_scope
            )

        # It's something else which we don't understand
        else:
            report_job_status_task.delay(
                FacebookJobStatus.GenericFacebookError, job_scope,
            )
        raise
    except Exception:
        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            FacebookJobStatus.GenericError, job_scope
        )
        raise
