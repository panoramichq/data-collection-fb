import functools
import gevent

from datetime import datetime, date
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from typing import Callable, Dict, Any, Generator

from facebook_business.adobjects.insightsresult import InsightsResult

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.tokens import PlatformTokenManager
from oozer.common.cold_storage import batch_store
from oozer.common.enum import ReportEntityApiKind, FB_AD_VIDEO_MODEL
from oozer.common.facebook_api import PlatformApiContext
from oozer.common.facebook_async_report import FacebookAsyncReportStatus
from oozer.common.job_scope import JobScope
from oozer.common.vendor_data import add_vendor_data

from oozer.metrics.constants import (
    ENUM_LEVEL_MAP,
    REPORT_TYPE_FB_BREAKDOWN_ENUM,
    DEFAULT_REPORT_FIELDS,
    VIDEO_REPORT_METRICS,
    VIDEO_REPORT_FIELDS,
)
from oozer.metrics.vendor_data_extractor import report_type_vendor_data_extractor_map


def _convert_and_validate_date_format(dt) -> str:
    """
    Converts incoming values that may represent a date
    into a FB-specific stringified date format
    that is acceptable for the `time_range` report parameter
    """
    # datetime is actually a subclass of date class
    # but for clarity of what we are doing, will check against
    # both, though only comparing to date is needed
    if not isinstance(dt, (date, datetime)):
        # we assume it's some string
        try:
            dt = datetime.strptime(dt, '%Y-%m-%d')
        except (ValueError, TypeError):
            raise ValueError(f"Value '{dt}' cannot be read as 'YYYY-MM-DD' string")
    return dt.strftime('%Y-%m-%d')


class JobScopeParsed:
    report_params: Dict[str, Any] = None
    datum_handler: Callable[[Dict[str, Any]], None] = None
    report_root_fb_entity = None
    report_entity_kind: str = None

    def __init__(self, job_scope: JobScope, report_entity_api_kind: str):
        if job_scope.report_type not in ReportType.ALL_METRICS:
            raise ValueError(
                f"Report type {job_scope.report_type} specified is not one of supported values: "
                + ReportType.ALL_METRICS
            )
        # cool. we are in the right place...

        self.report_params = {
            'fields': DEFAULT_REPORT_FIELDS,
            'action_attribution_windows': [
                # Default 'value' cannot be removed. It's always 1d_view PLUS 28d_click
                # but, our customers, especially Fandango, like just clicks
                # and at different windows. So, we ask for extra windows for all actions
                # The move windows we ask the data for, the less reliably it returns
                # Be super conservative about asking for more
                AdsInsights.ActionAttributionWindows.value_1d_view,  # requirement for Fandango
                # AdsInsights.ActionAttributionWindows.value_7d_view,  # nobody cared to ask for it
                AdsInsights.ActionAttributionWindows.value_28d_view,  # nice to have for Fandango
                AdsInsights.ActionAttributionWindows.value_1d_click,  # nice to have for Fandango
                # AdsInsights.ActionAttributionWindows.value_7d_click,  # nobody cared to ask for it
                AdsInsights.ActionAttributionWindows.value_28d_click,  # requirement for Fandango
            ],
        }

        # Next is (a) vs (b) - abstraction level determination
        is_per_parent_report = not job_scope.entity_id and job_scope.report_variant in Entity.ALL

        if is_per_parent_report:
            entity_id = job_scope.ad_account_id
            entity_type = Entity.AdAccount
            entity_type_reporting = job_scope.report_variant
            if report_entity_api_kind == ReportEntityApiKind.Ad:
                self.report_params.update(level=ENUM_LEVEL_MAP[job_scope.report_variant])
        else:
            # direct, per-entity report
            entity_id = job_scope.entity_id
            entity_type = job_scope.entity_type
            entity_type_reporting = job_scope.report_variant
            if report_entity_api_kind == ReportEntityApiKind.Ad:
                self.report_params.update(level=ENUM_LEVEL_MAP[entity_type_reporting])

        # Now, (c), (d), (e), (f), (g) choices
        # we already checked above that this is one of metrics report types
        # So we know it will be either lifetime or day-with-breakdown type
        # TODO: add fields listings appropriate for each type
        if job_scope.report_type == ReportType.lifetime:
            self.report_params.update(date_preset=AdsInsights.DatePreset.lifetime)
        elif job_scope.report_type in REPORT_TYPE_FB_BREAKDOWN_ENUM:  # some day-with-breakdown type
            self.report_params.update(
                time_increment=1,  # group by calendar day (in AA tz)
                time_range={
                    'since': _convert_and_validate_date_format(job_scope.range_start),
                    # No value for job_scope.range_end means 1-day report for range_start day
                    'until': _convert_and_validate_date_format(job_scope.range_end or job_scope.range_start),
                },
                breakdowns=REPORT_TYPE_FB_BREAKDOWN_ENUM[job_scope.report_type],
            )
        else:
            raise ValueError(
                f"Report type {job_scope.report_type} does not have a mapped Platform-side breakdown value."
            )

        # Indicates that datum returned in a per-parent report is by itself
        # naturally mapped to some single normative job ,
        # meaning each element can be stored separately
        # but only under normative ID computed on the fly
        # from the datum.
        # This must be accompanied by a transform fn that
        # derives a normative ID from data.

        # special case.
        # when report type is per-specific-single-entity-ID
        # AND one of per-day-with-breakdown
        # per-Entity-ID-per-day bundle with 24 records before saving it.
        # This results in a single write to the cold store under
        # single normative ID.
        is_whole_report_bundle_write = (
            # must be one of those per-day reports
            job_scope.report_type in ReportType.ALL_DAY_BREAKDOWNS
            and
            # except for DMA-based data, as these can be very long,
            # - 10s of thousands of records per day
            not job_scope.report_type == ReportType.day_dma
            and
            # and the report is per single entity ID
            job_scope.entity_id
            and not job_scope.report_variant
            and
            # and report is for a single calendar day
            # ReportType.ALL_DAY_BREAKDOWNS means there must be a non-Null
            # value in time_range, but we check anyway
            self.report_params['time_range']['since']
            and self.report_params['time_range']['since'] == self.report_params['time_range']['until']
        )

        # a more complex variant of whole_report_bundle_write
        # where, while we canNOT spool entire report into memory to
        # write it as one bundle, we cannot really write each
        # individual result out either, as there will be a shit-load of them
        # and we have to write is some sort of batching mode, but
        # cannot cleanly group the bundles into per-normative-ID bundles,
        # and instead will write under effective ID, but with a suffix
        # indicating the monotonically-increasing chunk number.

        # Disabled but kept for reference to compare to shorter version immediately below
        # These represent good range of choices for cold store handlers.
        # When / if there is value to it, steal from this commented out code.
        # if is_naturally_normative_child:
        #     self.datum_handler = batch_store.NaturallyNormativeChildStore(job_scope)
        # elif is_whole_report_bundle_write:
        #     self.datum_handler = batch_store.MemorySpoolStore(job_scope)
        # elif is_chunk_write:
        #     self.datum_handler = batch_store.ChunkDumpStore(job_scope)
        # else:
        #     self.datum_handler = batch_store.NormalStore(job_scope)

        # let's be more aggressive about doing bundled writes to cold store
        # and (temporarily) get away from "normative" and single-datum writes
        # There are two ways we can get closer to bundled writes:
        #  - spool entire report in memory and flush out at the end, when we know we can tolerate that
        #  - spool large chunks of report in memory and flush them periodically if we fear large sizes in report.
        if is_whole_report_bundle_write:
            self.datum_handler = batch_store.MemorySpoolStore(job_scope)
        else:
            self.datum_handler = batch_store.ChunkDumpStore(job_scope, chunk_size=200)

        with PlatformApiContext(job_scope.token) as fb_ctx:
            self.report_root_fb_entity = fb_ctx.to_fb_model(entity_id, entity_type)

        # here we configure code that will augment each datum with  record ID
        vendor_data_extractor = report_type_vendor_data_extractor_map[job_scope.report_type]
        if job_scope.report_type == ReportType.day_hour:
            # hour report type's ID extractor function needs extra leading arg - timezone
            vendor_data_extractor = functools.partial(vendor_data_extractor, job_scope.ad_account_timezone_name)

        aux_data = {
            'ad_account_id': job_scope.ad_account_id,
            'entity_type': entity_type_reporting,
            'report_type': job_scope.report_type,
        }

        self.augment_with_vendor_data = lambda data: add_vendor_data(data, **vendor_data_extractor(data, **aux_data))


class Insights:
    @staticmethod
    def iter_ads_insights(fb_entity: Any, report_params: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """
        Run the actual execution of the insights job for Ads API

        :param fb_entity: The ads api facebook entity instance
        :param report_params: FB API report params
        """
        report_status_obj: AdReportRun = fb_entity.get_insights(params=report_params, is_async=True)

        report_tracker = FacebookAsyncReportStatus(report_status_obj)

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
            gevent.sleep(report_tracker.backoff_interval)
            report_tracker.refresh()

        return report_tracker.iter_report_data()

    @classmethod
    def iter_collect_insights(cls, job_scope: JobScope, _):
        """
        Central, *GENERIC* implementation of insights fetcher task

        The goal of this method is to be the entry point for
        metrics fetching Celery tasks. This method is expected to parse
        the JobScope object, figure out that needs to be done
        based on data in the JobScope object and convert that data into
        proper parameters for calling FB

        :param job_scope: The JobScope as we get it from the task itself
        :param _: A job context we use for entity checksums
        """
        if not job_scope.tokens:
            raise ValueError(f"Job {job_scope.job_id} cannot proceed. No platform tokens provided.")

        token = job_scope.token
        # We don't use it for getting a token. Something else that calls us does.
        # However, we use it to report usages of the token we got.
        token_manager = PlatformTokenManager.from_job_scope(job_scope)

        scope_parsed = JobScopeParsed(job_scope, ReportEntityApiKind.Ad)
        data_iter = cls.iter_ads_insights(scope_parsed.report_root_fb_entity, scope_parsed.report_params)

        with scope_parsed.datum_handler as store:
            for cnt, datum in enumerate(data_iter):
                # this computes values for and adds _oprm data object
                # to each datum that passes through us.
                scope_parsed.augment_with_vendor_data(datum)

                store(datum)
                yield datum

                if cnt % 1000 == 0:
                    # default paging size for entities per parent
                    # is typically around 25. So, each 1000 results
                    # means about 40 hits to FB
                    token_manager.report_usage(token, 40)

        token_manager.report_usage(token)
