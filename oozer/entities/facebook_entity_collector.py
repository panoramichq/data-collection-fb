import random
from collections import namedtuple

from facebookads.api import FacebookRequestError
from typing import Generator

from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.job_context import JobContext
from oozer.common import cold_storage
from oozer.common.facebook_collector import FacebookCollector
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


class FacebookEntityCollector(FacebookCollector):

    def get_entities_for_adaccount(self, ad_account, entity_type, fields=None):
        """
        Generic getter for entities from the AdAccount edge

        :param str ad_account: Ad account id
        :param str entity_type:
        :param list fields: List of entity fields to fetch
        :return:
        """

        if entity_type not in Entity.ALL:
            raise ValueError(
                f'Value of "entity_type" argument must be one of {Entity.ALL}, got {entity_type} instead.'
            )
        FBModel = ENUM_VALUE_FB_MODEL_MAP[entity_type]

        ad_account = self._get_ad_account(ad_account)

        getter_method = {
            FB_CAMPAIGN_MODEL: ad_account.get_campaigns,
            FB_ADSET_MODEL: ad_account.get_ad_sets,
            FB_AD_MODEL: ad_account.get_ads
        }[FBModel]

        fields_to_fetch = fields or self._get_default_fileds(FBModel)
        yield from getter_method(fields=fields_to_fetch)

    @classmethod
    def checksum_entity(cls, entity):
        """
        Compute a hash of the entity fields that we consider stable, to be able
        to tell apart entities that have / have not changed in between runs.

        This method requires an intrinsic knowledge of "what the entity is".

        :return EntityHash: The hashes for the entity itself and
            and fields hashed
        """

        # Pick stable fields
        # Use non-cryptographic has over stable fields. (SeaHash?)

        return EntityHash(
            data=str(random.randint(1, 10000000)),
            fields='some_hash'
        )


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


def collect_entities_for_adaccount(entity_type, job_scope, job_context=None):
    """
    Collects an arbitrary entity for an ad account

    :param entity_type: The entity to collect for
    :param JobScope job_scope: The JobScope as we get it from the task itself
    :param JobContext job_context: A job context we use for entity checksums
    :rtype: Generator[Dict]
    """
    job_context = job_context or JobContext()

    # Report start of work
    report_job_status_task.delay(FacebookEntityJobStatus.Start, job_scope)

    try:
        with FacebookEntityCollector(job_scope.token) as collector:

            # Fetch us the entities iterator
            entities = collector.get_entities_for_adaccount(
                job_scope.ad_account_id,
                entity_type
            )

            # Report on the effective task status
            report_job_status_task.delay(
                FacebookEntityJobStatus.EntitiesFetched, job_scope
            )

            job_scope_base_data = job_scope.to_dict()

            for entity in entities:

                # Externalize these for clarity
                current_hash_raw = job_context.entity_checksums.get(
                    entity['id'], (None, None)
                )
                current_hash = EntityHash(*current_hash_raw)
                entity_hash = FacebookEntityCollector.checksum_entity(entity)

                # Check whether we actually need to put this into the ETL
                # pipeline it is possible that a given entity has not changed
                # since last time we checked and therefore it is not necessary
                # to deal with right now
                if current_hash == entity_hash:
                    continue

                entity_data = entity.export_all_data()
                normative_job_scope = JobScope(
                    job_scope_base_data,
                    entity_id=entity_data.get('id'),
                    entity_type=entity_type
                )

                # Signal to the system the new entity
                feedback_entity_task.delay(entity_data, entity_type, entity_hash)

                # Report on the normative task status
                report_job_status_task.delay(
                    FacebookEntityJobStatus.EntityFetched, normative_job_scope
                )

                # Store the individual datum, use job context for the cold
                # storage thing to divine whatever it needs from the job context
                cold_storage.store(entity_data, normative_job_scope)

                # Report on the normative task status
                report_job_status_task.delay(
                    FacebookEntityJobStatus.InColdStore, normative_job_scope
                )

                yield entity

        # Report on the effective task status
        report_job_status_task.delay(
            FacebookEntityJobStatus.InColdStore, job_scope
        )

    except FacebookRequestError as e:
        # Check for this
        # error_code = 100,  CodeException (error subcode: 1487534)
        # ^ means we asked for too much data

        # Inspect the exception for FB exceptions, so we can understand what's
        # going on
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
