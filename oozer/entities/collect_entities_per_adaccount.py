import xxhash

from collections import namedtuple
from datetime import datetime
from facebookads.adobjects import ad
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
from oozer.entities.feedback_entity_task import feedback_entity_task


class EntityHash(namedtuple('EntityHash', ['data', 'fields'])):
    """
    Container for the hash to make it a little bit nicer
    """

    def __eq__(self, other):
        """
        Add equality operator for easy checking
        """
        return self.data == other.data and self.fields == other.fields


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


def _checksum_entity(entity, fields=None):
    """
    Compute a hash of the entity fields that we consider stable, to be able
    to tell apart entities that have / have not changed in between runs.

    This method requires an intrinsic knowledge of "what the entity is".

    :return EntityHash: The hashes for the entity itself and
        and fields hashed
    """

    # Drop fields we don't care about
    blacklist = {
        FB_CAMPAIGN_MODEL: [],
        FB_ADSET_MODEL: [],
        FB_AD_MODEL: [ad.Ad.Field.recommendations]
    }

    fields = fields or get_default_fields(entity.__class__)

    # Run through blacklist
    fields = filter(lambda f: f not in blacklist[entity.__class__], fields)

    raw_data = entity.export_all_data()

    data_hash = xxhash.xxh64()
    fields_hash = xxhash.xxh64()

    for field in fields:
        data_hash.update(str(raw_data.get(field, '')))
        fields_hash.update(field)

    return EntityHash(
        data=data_hash.hexdigest(),
        fields=fields_hash.hexdigest()
    )


def _checksum_from_job_context(job_context, entity_id):
    """
    Recreate the EntityHash object from JobContext provided

    :param JobContext job_context: The provided job context
    :param string entity_id:

    :return EntityHash: The reconstructed EntityHash object
    """
    current_hash_raw = job_context.entity_checksums.get(
        entity_id, (None, None)
    )
    return EntityHash(*current_hash_raw)


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

        last_fetch_dt = datetime.utcnow()
        entities = iter_native_entities_per_adaccount(
            root_fb_entity,
            entity_type
        )

        # Report on the effective task status
        report_job_status_task.delay(
            FacebookEntityJobStatus.EntitiesFetched, job_scope
        )

        job_scope_base_data = job_scope.to_dict()

        for entity in entities:

            # Externalize these for clarity
            current_hash = _checksum_from_job_context(
                job_context, entity['id']
            )
            entity_hash = _checksum_entity(entity)
            entity_data = entity.export_all_data()

            normative_job_scope = JobScope(
                job_scope_base_data,
                entity_id=entity_data.get('id'),
                entity_type=entity_type
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

            yield entity

        # Report on the effective task status
        report_job_status_task.delay(
            FacebookEntityJobStatus.Done, job_scope
        )

    except FacebookRequestError as e:
        # Is this a throttling error?
        if FacebookApiErrorInspector.is_throttling_exception(e):
            report_job_status_task.delay(
                FacebookEntityJobStatus.ThrottlingError, job_scope
            )

        # Did we ask for too much data?
        elif FacebookApiErrorInspector.is_too_large_data_exception(e):
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
