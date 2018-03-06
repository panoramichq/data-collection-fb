import logging

from common.celeryapp import get_celery_app
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.console_api import ConsoleApi

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

    logger.info(
        f'{job_scope} started'
    )


    logger.info(job_context)

    console_client =  ConsoleApi(CONSOLE_API_TOKEN)

    for ad_account in console_client.get_active_accounts():
        print(ad_account)


