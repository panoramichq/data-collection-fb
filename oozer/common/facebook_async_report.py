from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights

from config.facebook import INSIGHTS_MAX_POLLING_INTERVAL, INSIGHTS_MIN_POLLING_INTERVAL


class FacebookReportDefinition:
    """

    """
    # TODO: Decide whether we need and want the notion of what fields constitute
    # a report

    # level = None
    # action_attribution_windows = None
    # action_breakdowns = None
    # action_report_time = None
    # fields = None
    # breakdowns = None
    # date_preset = None
    # default_summary = None
    # filtering = None
    # product_id_limit = None
    # sort = None
    # summary = None
    # summary_action_breakdowns = None
    # time_increment = None
    # time_range = None
    # time_ranges = None
    # use_account_attribution_setting = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return self.__dict__.copy()


class FacebookAsyncReportStatus:
    """
    Represents a remote Facebook report job
    Wraps an AdReportRun instance from FB Ads SDK
    Simplifies polling on / checking when report is done
    """

    class ReportFailed(ValueError):
        pass


    PENDING_STATE = {'Job Not Started', 'Job Started', 'Job Running'}
    SUCCEEDED_STATE = {'Job Completed',}
    FAILED_STATE = {'Job Failed', 'Job Skipped'}
    COMPLETED_STATE = SUCCEEDED_STATE | FAILED_STATE
    BACKOFF_MAX_REFRESH_COUNT = 10

    _report = None

    def __init__(self, report_status_obj):
        """
        :param AdReportRun report_status_obj:
        """
        # prior version of this method allowed spin up of
        # AdReportRun on the fly from report ID and Token.
        # That's actually cool because if we stay true to that
        # approach and manifest AdReportRun object on the fly
        # for every call, we can pass FacebookAsyncReportStatus
        # instance from Celery task to Celery task and have Celery
        # properly deserialize FacebookAsyncReportStatus
        # into a functional state.
        # When we hit that need, we may need to change internals
        # to store only report ID and token. Until then...
        self._report = report_status_obj
        self._refresh_count = 0

    def refresh(self):
        """
        Get fresh status of the report with FB
        """
        self._refresh_count += 1
        return self._report.remote_read()

    @property
    def status(self):
        return self._report.get(AdReportRun.Field.async_status)

    @property
    def is_complete(self):
        """
        Checks whether the given report finished being worked on. Mind that even
        failed states are also considered completed

        :return bool: Report completed somehow
        """
        return self.status in self.COMPLETED_STATE

    @property
    def backoff_interval(self):
        # Cap refresh count to avoid large powers
        refresh_count = min(self._refresh_count, self.BACKOFF_MAX_REFRESH_COUNT)
        return min(INSIGHTS_MAX_POLLING_INTERVAL, INSIGHTS_MIN_POLLING_INTERVAL * (2 ** refresh_count))

    @property
    def is_success(self):
        """
        Check whether given report finished being worked on and the operation
        was a success. If so, it is safe to start reading the report

        :return bool: Report completed successfully
        """
        return self.status in self.SUCCEEDED_STATE

    def iter_report_data(self, *args, **kwargs):
        """
        :return:
        """
        if not self.is_success:
            self.ReportFailed(f"Report is not marked as '{self.status}' - not ready for consumption.")

        # self._report.get_insights() returns a *GENERATOR*
        # that transparently pages behind the scenes
        # Do NOT use any serialization methods on self._report.get_insights() returned value
        # just iterate through it and you are guarantee to get them all that way.

        ads_insights_object = None  # type: AdsInsights
        for ads_insights_object in self._report.get_insights(*args, **kwargs):
            # .export_all_data converts AdsInsights back into pure dict
            # with native keys and *native* nested values.
            yield ads_insights_object.export_all_data()
