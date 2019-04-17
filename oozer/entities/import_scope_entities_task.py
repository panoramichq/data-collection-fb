import logging

from pynamodb.exceptions import PutError

from common.error_inspector import ErrorInspector
from common.measurement import Measure
from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from facebook_business.exceptions import FacebookRequestError
from common.page_tokens import PageTokenManager
from common.store.entities import AdAccountEntity, PageEntity
from common.tokens import PlatformTokenManager
from oozer.common.console_api import ConsoleApi
from oozer.common.facebook_api import PlatformApiContext, get_default_fields
from oozer.common.helpers import extract_tags_for_celery_fb_task
from oozer.common.job_scope import JobScope
from oozer.reporting import reported_task

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
@reported_task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task)
@Measure.counter(
    __name__, function_name_as_metric=True, count_once=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task
)
def import_ad_accounts_task(job_scope: JobScope, _):
    """
    Collect all facebook ad accounts that are active in the console api
    """

    assert job_scope.entity_type == Entity.Scope
    _get_good_token(job_scope)

    logger.info(f'{job_scope} started')

    return _import_entities_from_console(Entity.AdAccount, job_scope)


@app.task
@reported_task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task)
@Measure.counter(
    __name__, function_name_as_metric=True, count_once=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task
)
def import_pages_task(job_scope: JobScope, _):
    """
    Collect all facebook ad accounts that are active in the console api
    """

    assert job_scope.entity_type == Entity.Scope
    _get_good_token(job_scope)

    logger.info(f'{job_scope} started')

    return _import_entities_from_console(Entity.Page, job_scope)


def _get_good_token(job_scope: JobScope):
    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    token = job_scope.token
    if not token:
        raise ValueError(f"Job {job_scope.job_id} cannot proceed. No tokens provided.")


def _have_entity_access(entity_type: str, entity_id: str, access_token: str) -> bool:
    """
    Test if we have access to that entity. Returns True or throws fb request error.
    """
    with PlatformApiContext(access_token) as fb_ctx:
        entity = fb_ctx.to_fb_model(entity_id, entity_type)
        fields = get_default_fields(entity.__class__)
        entity_data = entity.remote_read(fields=fields)
        return True


def _import_entities_from_console(entity_type: str, job_scope: JobScope):
    page_token_manager = PageTokenManager(JobScope.namespace, job_scope.sweep_id)
    ad_account_token_manager = PlatformTokenManager(JobScope.namespace, job_scope.sweep_id)

    # TODO: Rethink registration of these.
    # effectively, even though we store scopes in DB, unless they are added
    # to code below, they don't exist. Seems kinda silly
    entity_type_map = {
        Entity.AdAccount: (
            ConsoleApi.get_accounts,
            AdAccountEntity,
            lambda account_id: ad_account_token_manager.get_best_token(),
        ),
        Entity.Page: (ConsoleApi.get_pages, PageEntity, lambda page_id: page_token_manager.get_best_token(page_id)),
    }

    if entity_type not in entity_type_map:
        # logging.warning(f'No registered AdAccount extractor API for scope "{job_scope.entity_id}"')
        # how about we raise in Celery Tasks and have a custom exception handler set at base Task
        raise ValueError(
            f'No registered entity extractor API for scope "{job_scope.entity_id}" and entity "{entity_type}"'
        )
    entity_extractor, entity_model, get_access_token = entity_type_map[entity_type]

    entities = entity_extractor(job_scope.token)
    imported_entities = 0
    for entity in _get_entities_to_import(entities, 'ad_account_id'):
        entity_id = entity['ad_account_id']
        is_active = entity.get('active', True)
        access_token = get_access_token(entity_id)
        is_accessible = False
        try:
            is_accessible = _have_entity_access(entity_type, entity_id, access_token)
        except FacebookRequestError as e:
            #  On purpose not sending to inspector, since that would result in 'unknown' exceptions in ddog.
            # We use other metric for tracking accounts that were not imported.
            logger.exception(f'Error when testing account accessibility {entity_type} {entity_id}')
        tags = {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'is_accessible': is_accessible,
            'is_active': is_active,
        }
        Measure.counter('console_entity_import', tags=tags).increment()

        try:
            logger.warning(f'Importing {entity_type} {entity_id} is_accessible: {is_accessible} is_active: {is_active}')
            entity_model.upsert_entity_from_console(job_scope, entity, is_accessible)
            imported_entities += 1
        except PutError as ex:
            if ErrorInspector.is_dynamo_throughput_error(ex):
                # just log and get out. Next time around we'll pick it up
                ErrorInspector.inspect(ex)
            else:
                raise
    return imported_entities


def _get_entities_to_import(entities, entity_id_key):
    """
    Fixes issue describe here
    https://operam.slack.com/archives/GC03M0PB5/p1548841927032100?thread_ts=1548803775.031500&cid=GC03M0PB5
    """
    grouped_entities = {}
    for entity in entities:
        entity_id = entity[entity_id_key]
        should_overwrite = True
        if entity_id in grouped_entities:
            should_overwrite = not grouped_entities[entity_id].get('active', True) and entities.get('active', True)

        if should_overwrite:
            grouped_entities[entity_id] = entity

    return grouped_entities.values()
