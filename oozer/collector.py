import random
import functools
import requests

from facebookads.api import FacebookAdsApi, FacebookSession
from facebookads.adobjects import abstractcrudobject, adaccount, campaign, \
    adset, ad, adreportrun


ENTITY_ADACCOUNT = adaccount.AdAccount
ENTITY_CAMPAIGN = campaign.Campaign
ENTITY_ADSET = adset.AdSet
ENTITY_AD = ad.Ad

ENTITY_NAMES = {
    ENTITY_ADACCOUNT: 'AA',
    ENTITY_CAMPAIGN: 'C',
    ENTITY_ADSET: 'AS',
    ENTITY_AD: 'A'
}


class FacebookReportDefinition:
    """

    """
    # TODO: Decide whether we need and want the notion of what fields constitue
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

    PENDING_STATE = ['Job Not Started', 'Job Started', 'Job Running']
    SUCCEEDED_STATE = 'Job Completed'
    FAILED_STATE = ['Job Failed', 'Job Skipped']

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
        if self._status == self.SUCCEEDED_STATE or \
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


class FacebookCollector:
    """
    A simple wrapper for Facebook SDK, using local API sessions as not to
    pollute the the global default API session with initialization
    """

    def __init__(self, token):
        """

        :param token:
        """
        self.token = token

    def __enter__(self):
        """

        """
        self.api = FacebookAdsApi(FacebookSession(access_token=self.token))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        We do not need to do anything specific yet
        """
        pass

    def _get_ad_account(self, account_id):
        """
        Construct the Ad Account object using the id without the `act_` bit

        :param account_id:
        :return:
        """
        return adaccount.AdAccount(fbid=f'act_{account_id}', api=self.api)

    def _get_default_fileds(self, entity):
        """
        Obtain default fields for a given entity type. Note that the entity
        class must come from the Facebook SDK

        :param entity:
        :return:
        """
        assert issubclass(entity, abstractcrudobject.AbstractCrudObject)

        return filter(
            lambda val: not val.startswith('__'),
            dir(entity.Field)
        )

    def get_entities_for_adaccount(self, ad_account, entity_type, fields=None):
        """
        Generic getter for entities from the AdAccount edge

        :param string ad_account: Ad account id
        :param entity_type:
        :param list fields: List of entity fields to fetch
        :return:
        """
        ad_account = self._get_ad_account(ad_account)

        fields_to_fetch = fields or self._get_default_fileds(entity_type)

        getter_method = {
            ENTITY_CAMPAIGN: ad_account.get_campaigns,
            ENTITY_ADSET: ad_account.get_ad_sets,
            ENTITY_AD: ad_account.get_ads
        }[entity_type]

        yield from getter_method(fields=fields_to_fetch)

    def get_insights(self, edge_entity, entity_id, report_params):
        """

        :param edge_entity:
        :param entity_id:
        :param report_params:
        :return FacebookAsyncReport:
        """
        if edge_entity is ENTITY_ADACCOUNT:
            edge_instance = self._get_ad_account(entity_id)
        else:
            edge_instance = edge_entity(fbid=entity_id)

        result = edge_instance.get_insights_async(params=report_params)

        # Check for this
        # error_code = 100,  CodeException (error subcode: 1487534)
        # ^ means we asked for too much data

        return FacebookAsyncReport(result['id'], self.token)

    @classmethod
    def checksum_entity(cls, entity):
        """
        Compute a hash of the entity fields that we consider stable, to be able
        to tell apart entities that have / have not changed in between runs

        :return (entity_hash, fields_hash): The hashes for the entity itself and
            and fields hashed
        """
        entity_hash = str(random.randint(1, 10000000))
        fields_hash = 'some_hash'

        # Pick stable fields

        # Use non-cryptographic has over stable fields. (SeaHash?)

        return entity_hash, fields_hash

    @classmethod
    def checksum_valid(cls, current_hash, hash_to_check):
        """
        TODO: For the time being, we always intentionally break the has

        :param string current_hash:
        :param tuple hash_to_check:
        :return bool: Entity has not changed
        """
        return False


STAGE_CONTEXT = {}


def collect_entities_for_adaccount(
    token, adaccount_id, entity_type, job_context, context,
    report_status, feedback_entity,
):
    """
    Collects an arbitrary entity for an ad account

    :param token:
    :param adaccount_id:
    :param entity_type:
    :param job_context:
    :param context:
    :param callable report_status:
    :param callable feedback_entity:
    :return:
    """
    # Setup the job reporting function, so that we can report on individual
    # entities and do not need to repeat the signature

    report_status = functools.partial(
        report_status, ENTITY_NAMES[entity_type],
    )

    with FacebookCollector(token) as collector:
        for entity in \
                collector.get_entities_for_adaccount(adaccount_id, entity_type):

            # TODO: STATUS: Figure out staging (we got data)
            report_status(entity['id'], 10, STAGE_CONTEXT)

            # Externalize these for clarity
            current_hash = context.get(entity['id'])
            entity_hash = FacebookCollector.checksum_entity(entity)

            # Check whether we actually need to put this into the ETL pipeline
            # It is possible that a given entity has not changed since last time
            # we checked and therefore it is not necessary to deal with right
            # now
            if FacebookCollector.checksum_valid(current_hash, entity_hash):
                # TODO: STATUS: Figure out staging (we are done, since we are skipping)
                report_status(entity['id'], 1000, STAGE_CONTEXT)
                continue

            # TODO: STATUS: Figure out staging (we are uploading data)
            report_status(entity['id'], 100, STAGE_CONTEXT)

            # TODO: STORAGE: push data to cold store

            # TODO: STATUS: Figure out staging (data uploaded)
            report_status(entity['id'], 110, STAGE_CONTEXT)

            # Signal to the system the new entity
            feedback_entity(entity, entity_hash)


def collect_insights(
    token, adaccount_id, insights_edge, report_definition , job_context,
    status_reporter
):
    """

    :param token:
    :param adaccount_id:
    :param insights_edge:
    :param job_context:
    :param job_context:
    :param callable status_reporter:
    :return:
    """

    pass
