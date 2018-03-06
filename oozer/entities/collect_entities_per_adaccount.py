from facebookads.api import FacebookRequestError
from typing import Generator

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from oozer.common import cold_storage
from oozer.common.facebook_api import (
    FacebookApiContext,
    FacebookApiErrorInspector,
    get_default_fields,
)
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status import JobStatus
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import (
    FB_CAMPAIGN_MODEL,
    FB_ADSET_MODEL,
    FB_AD_MODEL,
    ENUM_VALUE_FB_MODEL_MAP
)
from oozer.entities.entity_hash import _checksum_entity, _checksum_from_job_context
from oozer.entities.feedback_entity_task import feedback_entity_task


def iter_native_entities_per_adaccount(ad_account, entity_type, fields=None):
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

    yield from getter_method(fields=fields_to_fetch)


class FacebookEntityJobStatus(JobStatus):
    """
    Use this to communicate to give status reporter enough information to
    figure out what the stage id means in terms of failures
    """

    # Progress states
    Start = 100
    EntitiesFetched = 200
    EntityFetched = 200
    InColdStore = 500

    # Various error states
    TooMuchData = (-500, FailureBucket.TooLarge)
    ThrottlingError = (-700, FailureBucket.Throttling)
    GenericFacebookError = -900
    GenericError = -1000


def iter_collect_entities_per_adaccount(job_scope, job_context):
    """
    Collects an arbitrary entity for an ad account

    :param JobScope job_scope: The JobScope as we get it from the task itself
    :param JobContext job_context: A job context we use for entity checksums
    :rtype: Generator[Dict]
    """

    # This handler specifically expects to do per-parent
    # entity fetching, thus requiring proper entity enum in report_variant
    if job_scope.report_variant not in Entity.ALL:
        raise ValueError(
            f"Report level {job_scope.report_variant} specified is not one of supported values: {Entity.ALL}"
        )

    entity_type = job_scope.report_variant

    report_job_status_task.delay(FacebookEntityJobStatus.Start, job_scope)

    try:
        with FacebookApiContext(job_scope.token) as fb_ctx:
            root_fb_entity = fb_ctx.to_fb_model(
                job_scope.ad_account_id, Entity.AdAccount
            )

        entities = iter_native_entities_per_adaccount(
            root_fb_entity,
            entity_type
        )

        # Report on the effective task status
        report_job_status_task.delay(
            FacebookEntityJobStatus.EntitiesFetched, job_scope
        )

        job_scope_base_data = job_scope.to_dict()
        job_scope_base_data.update(
            entity_type=entity_type,
            is_derivative=True, # this keeps the scope from being counted as done task by looper
            report_variant=None,
        )

        for entity in entities:

            # Externalize these for clarity
            current_hash = _checksum_from_job_context(
                job_context, entity['id']
            )
            entity_hash = _checksum_entity(entity)
            entity_data = entity.export_all_data()

            normative_job_scope = JobScope(
                job_scope_base_data,
                entity_id=entity_data.get('id')
            )

            # Check whether we actually need to put this into the ETL
            # pipeline it is possible that a given entity has not changed
            # since last time we checked and therefore it is not necessary
            # to deal with right now
            if current_hash == entity_hash:
                report_job_status_task.delay(
                    FacebookEntityJobStatus.Done, normative_job_scope
                )
                # Don't need to save it / send it to cold store
                continue  # to next entity
            else:
                report_job_status_task.delay(
                    FacebookEntityJobStatus.EntityFetched, normative_job_scope
                )

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            cold_storage.store(entity_data, normative_job_scope)

            # Signal to the system the new entity
            feedback_entity_task.delay(entity_data, entity_type, entity_hash)

            report_job_status_task.delay(
                FacebookEntityJobStatus.Done, normative_job_scope
            )

            yield entity_data

        # Report on the effective task status
        report_job_status_task.delay(
            FacebookEntityJobStatus.Done, job_scope
        )

    except FacebookRequestError as e:
        # Build ourselves the error inspector
        inspector = FacebookApiErrorInspector(e)

        # Is this a throttling error?
        if inspector.is_throttling_exception():
            report_job_status_task.delay(
                FacebookEntityJobStatus.ThrottlingError, job_scope
            )

        # Did we ask for too much data?
        elif inspector.is_too_large_data_exception():
            report_job_status_task.delay(
                FacebookEntityJobStatus.TooMuchData, job_scope
            )

        # It's something else which we don't understand
        else:
            report_job_status_task.delay(
                FacebookEntityJobStatus.GenericFacebookError, job_scope,
            )
        raise

    except Exception:
        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            FacebookEntityJobStatus.GenericError, job_scope
        )
        raise
