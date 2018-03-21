from twitter_ads.cursor import Cursor

from common.twitter.enums.entity import Entity

from oozer.common import cold_storage
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.twitter_api import TwitterApiContext

import oozer.entities.entity_hash as entity_hash

from oozer.common.enum import TwitterJobStatus

from config.twitter import TOKEN, SECRET, CONSUMER_KEY, CONSUMER_SECRET


def iter_native_entities_per_adaccount(ad_account, entity_type):
    """
    Generic getter for entities from the AdAccount edge

    :param AdAccount ad_account: Ad account id
    :param str entity_type:
    :param list fields: List of entity fields to fetch
    :return Cursor:
    """

    if entity_type not in Entity.ALL:
        raise ValueError(
            f'Value of "entity_type" argument must be one of {Entity.ALL}, '
            f'got {entity_type} instead.'
        )

    getter_method = {
        Entity.Campaign: ad_account.campaigns,
        Entity.LineItem: ad_account.line_items,
        Entity.PromotedTweet: ad_account.promoted_tweets
    }[entity_type]

    yield from getter_method()


def iter_collect_entities_per_adaccount(job_scope, job_context):
    report_job_status_task.delay(TwitterJobStatus.Start, job_scope)

    if job_scope.report_variant not in Entity.ALL:
        raise ValueError(
            f"Report level {job_scope.report_variant} specified is not one of supported values: {Entity.ALL}"
        )

    entity_type = job_scope.report_variant

    # Fixme: replace this with token manager instead PlatformTokenManager.from_job_scope(job_scope)
    token, secret, consumer_key, consumer_secret = TOKEN, SECRET, CONSUMER_KEY, CONSUMER_SECRET

    try:
        with TwitterApiContext(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET) as tw_ctx:
            root_entity = tw_ctx.to_tw_model(job_scope.ad_account_id, Entity.AdAccount)

        entities = iter_native_entities_per_adaccount(
            root_entity,
            entity_type
        )

        job_scope_base_data = job_scope.to_dict()
        job_scope_base_data.update(
            entity_type=entity_type,
            is_derivative=True,
            report_variant=None,
        )

        cnt = 0

        for entity in entities:

            # FIXME: modify hashing for Twitter entities
            current_checksum = entity_hash.checksum_from_job_context(
                job_context, entity.id
            )

            entity_checksum = entity_hash.checksum_entity(entity)
            entity_data = entity.to_params()

            normative_job_scope = JobScope(
                job_scope_base_data,
                entity_id=entity_data.get('id')
            )

            # Check whether we actually need to put this into the ETL
            # pipeline it is possible that a given entity has not changed
            # since last time we checked and therefore it is not necessary
            # to deal with right now
            if current_checksum == entity_checksum:
                report_job_status_task.delay(
                    TwitterJobStatus.Done, normative_job_scope
                )
                # Don't need to save it / send it to cold store
                continue  # to next entity
            else:
                report_job_status_task.delay(
                    TwitterJobStatus.DataFetched, normative_job_scope
                )

            # Store the individual datum, use job context for the cold
            # storage thing to divine whatever it needs from the job context
            cold_storage.store(entity_data, normative_job_scope)

            # FIXME: Signal the new entity to the system
            # feedback_entity_task.delay(entity_data, entity_type, entity_hash)

            report_job_status_task.delay(
                TwitterJobStatus.Done, normative_job_scope
            )

            yield entity_data

            cnt += 1

        report_job_status_task.delay(
            TwitterJobStatus.Done,
            job_scope
        )

    except Exception:
        report_job_status_task(TwitterJobStatus.GenericError, job_scope)
        raise


