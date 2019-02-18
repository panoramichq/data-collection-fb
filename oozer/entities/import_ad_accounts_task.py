import logging

from collections import defaultdict
from pynamodb.exceptions import PutError

from common.measurement import Measure
from common.bugsnag import BugSnagContextData
from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from common.store.entities import AdAccountEntity
from common.store.scope import DEFAULT_SCOPE
from common.tokens import PlatformTokenManager
from oozer.common.console_api import ConsoleApi
from oozer.common.enum import JobStatus
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task


app = get_celery_app()
logger = logging.getLogger(__name__)


# TODO: Rethink registration of these.
# effectively, even though we store scopes in DB, unless they are added
# to code below, they don't exist. Seems kinda silly
scope_api_map = {
    DEFAULT_SCOPE: ConsoleApi
}


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def import_ad_accounts_task(job_scope, job_context):
    """
    Collect all facebook ad accounts that are active in the console api

    :param JobScope job_scope: The dict representation of JobScope
    :param JobContext job_context:
    """

    try:
        assert job_scope.entity_type == Entity.Scope

        if not job_scope.tokens:
            good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
            if good_token is not None:
                job_scope.tokens = [good_token]

        token = job_scope.token
        if not token:
            raise ValueError(
                f"Job {job_scope.job_id} cannot proceed. No tokens provided."
            )

        Api = scope_api_map.get(job_scope.entity_id)
        if not Api:
            # logging.warning(f'No registered AdAccount extractor API for scope "{job_scope.entity_id}"')
            # how about we raise in Celery Tasks and have a custom exception handler set at base Task
            raise ValueError(f'No registered AdAccount extractor API for scope "{job_scope.entity_id}"')
        api = Api(token)

        logger.info(
            f'{job_scope} started'
        )

    except Exception as ex:
        # logger.exception(str(ex))
        report_job_status_task.delay(JobStatus.GenericError, job_scope)
        raise

    report_job_status_task.delay(JobStatus.Start, job_scope)

    try:
        accounts = api.get_accounts()
        for ad_account in accounts:
            # TODO: maybe create a normative job scope that says ("extracting ad account")
            try:
                # TODO: maybe rather get the entity first and update insert accordingly / if it has change
                AdAccountEntity.upsert(
                    job_scope.entity_id,  # scope ID
                    ad_account['ad_account_id'],
                    is_active=ad_account.get('active', True),
                    updated_by_sweep_id=job_scope.sweep_id
                )
            except PutError as ex:
                # TODO: ? report_job_status_task.delay(ConsoleExtractionJobStatus.UpsertError, normative_job_scope)
                ex_str = str(ex)
                if 'ProvisionedThroughputExceededException' in ex_str:
                    # just log and get out. Next time around we'll pick it up
                    logger.info(ex_str)
                else:
                    raise

        # TODO: Here iterate over the AdAccountEntity table
        # per this particular scope ID and for all AdAccount records
        # that don't have updated_by_sweep_id==job_scope.sweep_id mark inactive

        report_job_status_task.delay(JobStatus.Done, job_scope)
    except Exception as ex:
        BugSnagContextData.notify(ex, job_scope=job_scope)
        logger.error(str(ex))
        report_job_status_task.delay(JobStatus.GenericError, job_scope)
