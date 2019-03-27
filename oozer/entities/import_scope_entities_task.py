import logging

from pynamodb.exceptions import PutError

from common.measurement import Measure
from common.bugsnag import BugSnagContextData
from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from common.store.entities import AdAccountEntity, PageEntity
from common.tokens import PlatformTokenManager
from oozer.common.console_api import ConsoleApi
from oozer.common.enum import JobStatus
from oozer.common.helpers import extract_tags_for_celery_fb_task
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task)
@Measure.counter(
    __name__, function_name_as_metric=True, count_once=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task
)
def import_ad_accounts_task(job_scope: JobScope, _):
    """
    Collect all facebook ad accounts that are active in the console api
    """
    try:
        assert job_scope.entity_type == Entity.Scope
        _get_good_token(job_scope)

        logger.info(f'{job_scope} started')
    except Exception:
        report_job_status_task.delay(JobStatus.GenericError, job_scope)
        raise

    _import_entities_from_console(Entity.AdAccount, job_scope)


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task)
@Measure.counter(
    __name__, function_name_as_metric=True, count_once=True, extract_tags_from_arguments=extract_tags_for_celery_fb_task
)
def import_pages_task(job_scope: JobScope, _):
    """
    Collect all facebook ad accounts that are active in the console api
    """
    try:
        assert job_scope.entity_type == Entity.Scope
        _get_good_token(job_scope)

        logger.info(f'{job_scope} started')
    except Exception:
        report_job_status_task.delay(JobStatus.GenericError, job_scope)
        raise

    _import_entities_from_console(Entity.Page, job_scope)


def _get_good_token(job_scope: JobScope):
    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    token = job_scope.token
    if not token:
        raise ValueError(f"Job {job_scope.job_id} cannot proceed. No tokens provided.")


def _import_entities_from_console(entity_type: str, job_scope: JobScope):
    # TODO: Rethink registration of these.
    # effectively, even though we store scopes in DB, unless they are added
    # to code below, they don't exist. Seems kinda silly
    entity_type_map = {
        Entity.AdAccount: (ConsoleApi.get_accounts, AdAccountEntity),
        Entity.Page: (ConsoleApi.get_pages, PageEntity),
    }

    report_job_status_task.delay(JobStatus.Start, job_scope)

    if entity_type not in entity_type_map:
        # logging.warning(f'No registered AdAccount extractor API for scope "{job_scope.entity_id}"')
        # how about we raise in Celery Tasks and have a custom exception handler set at base Task
        raise ValueError(
            f'No registered entity extractor API for scope "{job_scope.entity_id}" and entity "{entity_type}"'
        )
    entity_extractor, entity_model = entity_type_map[entity_type]

    try:
        entities = entity_extractor(job_scope.token)
        for entity in _get_entities_to_import(entities, 'ad_account_id'):
            # TODO: maybe create a normative job scope that says ("extracting ad account")
            try:
                # TODO: maybe rather get the entity first and update insert accordingly / if it has change
                entity_model.upsert_entity_from_console(job_scope, entity)
            except PutError as ex:
                # TODO: ? report_job_status_task.delay(ConsoleExtractionJobStatus.UpsertError, normative_job_scope)
                ex_str = str(ex)
                if 'ProvisionedThroughputExceededException' in ex_str:
                    # just log and get out. Next time around we'll pick it up
                    logger.info(ex_str)
                else:
                    raise

        report_job_status_task.delay(JobStatus.Done, job_scope)
    except Exception as ex:
        BugSnagContextData.notify(ex, job_scope=job_scope)
        logger.error(str(ex))
        report_job_status_task.delay(JobStatus.GenericError, job_scope)


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
