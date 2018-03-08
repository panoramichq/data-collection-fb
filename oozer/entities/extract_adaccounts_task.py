import logging

from common.celeryapp import get_celery_app
from oozer.common.enum import JobStatus
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.console_api import ConsoleApi
from common.store.entities import FacebookAdAccountEntity

from oozer.common.report_job_status_task import report_job_status_task
from common.store.scope import FacebookAdAccountScope, FacebookToken, DEFAULT_SCOPE

from config.operam_console_api import TOKEN as CONSOLE_API_TOKEN

app = get_celery_app()
logger = logging.getLogger(__name__)

@app.task
def extract_adaccounts_task(job_scope, job_context):
    """
    Collect all facebook ad accounts that are active in the console api

    :param JobScope job_scope: The dict representation of JobScope
    :param JobContext job_context:
    """

    from pynamodb.exceptions import PutError


    logger.info(
        f'{job_scope} started'
    )
    report_job_status_task.delay(JobStatus.Start, job_scope)

    try:
        console_client = ConsoleApi(CONSOLE_API_TOKEN)
        accounts = console_client.get_active_accounts()

        for ad_account in accounts:
            # TODO: maybe create a normative job scope that says ("extracting ad account")
            try:
                # TODO: maybe rather get the entity first and update insert accordingly / if it has change
                record = FacebookAdAccountEntity.upsert(
                    DEFAULT_SCOPE,
                    ad_account['ad_account_id'],
                    is_active=True,
                    timezone=ad_account['timezone'],
                    updated_by_sweep_id=job_scope.sweep_id
                )
            except PutError as ex:
                # TODO: report_job_status_task.delay(ConsoleExtractionJobStatus.UpsertError, normative_job_scope)
                ex_str = str(ex)
                if 'ProvisionedThroughputExceededException' in ex_str:
                    logger.info(ex_str)
                else:
                    raise


        report_job_status_task.delay(JobStatus.Done, job_scope)
    except Exception as ex:
        logger.error(str(ex))
        report_job_status_task.delay(JobStatus.GenericError, job_scope)
