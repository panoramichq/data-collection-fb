from facebookads.adobjects import adreportrun


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
        self._params = kwargs

    def as_params_dict(self):
        return self._params


class FacebookAsyncReport:
    """
    Represents a remote Facebook report job
    """

    PENDING_STATE = {'Job Not Started', 'Job Started', 'Job Running'}
    SUCCEEDED_STATE = {'Job Completed',}
    FAILED_STATE = {'Job Failed', 'Job Skipped'}

    _token = None
    _report = None
    _status = None

    def __init__(self, report_run_id, access_token):
        """
        Construct the FB Ad Report run wrapper

        :param string report_run_id: The id of the remote report job
        :param string access_token: Facebook access token
        """
        self._token = access_token
        self._report = adreportrun.AdReportRun(fbid=report_run_id)

    def refresh(self):
        """
        Get fresh status of the report with FB
        """
        data = self._report.remote_read()
        self._status = data[
            adreportrun.AdReportRun.Field.async_status
        ]

    def completed(self):
        """
        Checks whether the given report finished being worked on. Mind that even
        failed states are also considered completed

        :return bool: Report completed somehow
        """
        if self._status in self.SUCCEEDED_STATE or \
            self._status in self.FAILED_STATE:

            return True

        return False

    def success(self):
        """
        Check whether given report finished being worked on and the operation
        was a success. If so, it is safe to start reading the report

        :return bool: Report completed successfully
        """
        return self._status == self.SUCCEEDED_STATE

    def read(self):
        """
        Read the remote generated report

        :return:
        # """
        # requests.get(
        #     url=REPORT_EXPORT_PATH,
        #     params={
        #         'format': 'csv',
        #         'report_run_id': self._report.id,
        #         'access_token': self._token
        #     },
        #     stream=True
        # )
