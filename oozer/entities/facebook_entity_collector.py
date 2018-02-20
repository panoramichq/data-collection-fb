import random
import functools

from oozer.common.facebook_collector import FacebookCollector
from oozer.common.enum import (
    ENTITY_CAMPAIGN,
    ENTITY_ADSET,
    ENTITY_AD,
    ENTITY_NAMES
)


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

    with FacebookEntityCollector(token) as collector:
        for entity in \
                collector.get_entities_for_adaccount(adaccount_id, entity_type):

            # TODO: STATUS: Figure out staging (we got data)
            report_status(entity['id'], 10, STAGE_CONTEXT)

            # Externalize these for clarity
            current_hash = context.get(entity['id'])
            entity_hash = FacebookEntityCollector.checksum_entity(entity)

            # Check whether we actually need to put this into the ETL pipeline
            # It is possible that a given entity has not changed since last time
            # we checked and therefore it is not necessary to deal with right
            # now
            if FacebookEntityCollector.checksum_valid(current_hash, entity_hash):
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
