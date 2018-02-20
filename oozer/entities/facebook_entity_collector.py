import random

from facebookads.api import FacebookRequestError

from oozer.common import cold_storage
from oozer.common.report_job_status_task import report_job_status
from oozer.common.facebook_collector import FacebookCollector
from oozer.common.enum import (
    ENTITY_CAMPAIGN,
    ENTITY_ADSET,
    ENTITY_AD,
)
from oozer.entities.entity_feedback_task import feedback_entity


class FacebookEntityCollector(FacebookCollector):

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


# Use this to communicate to give status reporter enough information to
# figure out what the stage id means in terms of failures
JOB_STATUS_REPORT_CONTEXT = {}


def collect_entities_for_adaccount(entity_type, job_scope, context):
    """
    Collects an arbitrary entity for an ad account

    :param entity_type: The entity to collect for
    :param dict job_scope: The dict representation of JobScope
    :param context:
    """

    # Report start of work
    report_job_status.delay(10, job_scope)

    try:
        with FacebookEntityCollector(job_scope['access_token']) as collector:
            for entity in \
                    collector.get_entities_for_adaccount(
                        job_scope['adaccount_id'], entity_type
                    ):

                # Externalize these for clarity
                current_hash = context.get(entity['id'])
                entity_hash = FacebookEntityCollector.checksum_entity(entity)

                # Check whether we actually need to put this into the ETL
                # pipeline it is possible that a given entity has not changed
                # since last time we checked and therefore it is not necessary
                # to deal with right now
                if FacebookEntityCollector.checksum_valid(
                    current_hash, entity_hash
                ):
                    continue

                # Store the individual datum, use job context for the cold
                # storage thing to divine whatever it needs from the job context
                cold_storage.store(dict(entity), job_scope)

                # Signal to the system the new entity
                feedback_entity.delay(entity, entity_hash)

                # Report some start of work
                report_job_status.delay(1000, job_scope)
    except FacebookRequestError as e:
        # Inspect the exception for FB exceptions, so we can distill the proper
        # failure bucket
        report_job_status.delay(-500, job_scope, JOB_STATUS_REPORT_CONTEXT)
        raise
    except Exception:
        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status.delay(-1000, job_scope, JOB_STATUS_REPORT_CONTEXT)
        raise
