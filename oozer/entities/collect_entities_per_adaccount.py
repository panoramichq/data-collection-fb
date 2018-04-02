from typing import Generator, Optional, Union
from facebookads.exceptions import FacebookError

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.tokens import PlatformTokenManager
from common.id_tools import generate_universal_id
from oozer.common.cold_storage.batch_store import ChunkDumpStore
from oozer.common.vendor_data import add_vendor_data
from oozer.common.facebook_api import (
    FacebookApiContext,
    FacebookApiErrorInspector,
    get_default_page_size,
    get_default_fields,
    get_default_status
)
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import (
    FB_ADACCOUNT_MODEL,
    FB_CAMPAIGN_MODEL,
    FB_ADSET_MODEL,
    FB_AD_MODEL,
    ENUM_VALUE_FB_MODEL_MAP,
    FacebookJobStatus
)
from oozer.entities.entity_hash import _checksum_entity, _checksum_from_job_context
from oozer.entities.feedback_entity_task import feedback_entity_task




def iter_native_entities_per_adaccount(ad_account, entity_type, fields=None, status=None, page_size=200):
    # type: (FB_ADACCOUNT_MODEL, str, Optional[list]) -> Generator[Union[FB_CAMPAIGN_MODEL, FB_ADSET_MODEL, FB_AD_MODEL]]
    """
    Generic getter for entities from the AdAccount edge

    :param AdAccount ad_account: Ad account id
    :param str entity_type:
    :param list fields: List of entity fields to fetch
    :return:
    """

    if entity_type not in Entity.ALL:
        raise ValueError(
            f'Value of "entity_type" argument must be one of {Entity.ALL}, '
            f'got {entity_type} instead.'
        )

    FBModel = ENUM_VALUE_FB_MODEL_MAP[entity_type]

    getter_method = {
        FB_CAMPAIGN_MODEL: ad_account.get_campaigns,
        FB_ADSET_MODEL: ad_account.get_ad_sets,
        FB_AD_MODEL: ad_account.get_ads
    }[FBModel]

    fields_to_fetch = fields or get_default_fields(FBModel)
    status = status or get_default_status(FBModel)
    page_size = page_size or get_default_page_size(FBModel)

    params = dict(
        summary=False
    )

    if page_size:
        params['limit'] = page_size

    if status:
        params['effective_status'] = status

    yield from getter_method(
        fields=fields_to_fetch,
        params=params
    )


def iter_collect_entities_per_adaccount(job_scope, job_context):
    """
    Collects an arbitrary entity for an ad account

    :param JobScope job_scope: The JobScope as we get it from the task itself
    :param JobContext job_context: A job context we use for entity checksums
    :rtype: Generator[Dict]
    """
    report_job_status_task.delay(FacebookJobStatus.Start, job_scope)

    try:
        # This handler specifically expects to do per-parent
        # entity fetching, thus requiring proper entity enum in report_variant
        if job_scope.report_variant not in Entity.ALL:
            raise ValueError(
                f"Report level {job_scope.report_variant} specified is not one of supported values: {Entity.ALL}"
            )

        entity_type = job_scope.report_variant

        token = job_scope.token
        if not token:
            raise ValueError(
                f"Job {job_scope.job_id} cannot proceed. No platform tokens provided."
            )

        # We don't use it for getting a token. Something else that calls us does.
        # However, we use it to report usages of the token we got.
        token_manager = PlatformTokenManager.from_job_scope(job_scope)

    except Exception:
        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            FacebookJobStatus.GenericError, job_scope
        )
        raise

    try:
        with FacebookApiContext(token) as fb_ctx:
            root_fb_entity = fb_ctx.to_fb_model(
                job_scope.ad_account_id, Entity.AdAccount
            )

        entities = iter_native_entities_per_adaccount(
            root_fb_entity,
            entity_type
        )

        record_id_base_data = job_scope.to_dict()
        record_id_base_data.update(
            entity_type=entity_type,
            report_variant=None,
        )

        with ChunkDumpStore(job_scope, chunk_size=200) as store:
            cnt = 0
            for entity in entities:
                entity_data = entity.export_all_data()
                entity_data = add_vendor_data(
                    entity_data,
                    id=generate_universal_id(
                        entity_id=entity_data.get(entity.Field.id),
                        **record_id_base_data
                    )
                )

                # Store the individual datum, use job context for the cold
                # storage thing to divine whatever it needs from the job context
                store(entity_data)

                # Signal to the system the new entity
                feedback_entity_task.delay(entity_data, entity_type, [None, None])

                yield entity_data
                cnt += 1

                if cnt % 100 == 0:
                    report_job_status_task.delay(
                        FacebookJobStatus.DataFetched, job_scope
                    )
                    # default paging size for entities per parent
                    # is typically around 25. So, each 100 results
                    # means about 4 hits to FB
                    token_manager.report_usage(token, 4)

        # Report on the effective task status
        report_job_status_task.delay(
            FacebookJobStatus.Done, job_scope
        )
        token_manager.report_usage(token)

    except FacebookError as e:
        # Build ourselves the error inspector
        inspector = FacebookApiErrorInspector(e)

        # Is this a throttling error?
        if inspector.is_throttling_exception():
            failure_status = FacebookJobStatus.ThrottlingError
            failure_bucket = FailureBucket.Throttling

        # Did we ask for too much data?
        elif inspector.is_too_large_data_exception():
            failure_status = FacebookJobStatus.TooMuchData
            failure_bucket = FailureBucket.TooLarge

        # It's something else which we don't understand
        else:
            failure_status = FacebookJobStatus.GenericFacebookError
            failure_bucket = FailureBucket.Other

        report_job_status_task.delay(failure_status, job_scope)
        token_manager.report_usage_per_failure_bucket(token, failure_bucket)
        raise

    except Exception:
        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            FacebookJobStatus.GenericError, job_scope
        )
        token_manager.report_usage_per_failure_bucket(token, FailureBucket.Other)
        raise
