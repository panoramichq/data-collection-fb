import logging

from common.celeryapp import get_celery_app
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.console_api import ConsoleApi
from common.store.entities import FacebookAdAccountEntity

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

    logger.info(
        f'{job_scope} started'
    )

    console_client = ConsoleApi(CONSOLE_API_TOKEN)
    accounts = console_client.get_active_accounts()

    from pprint import pprint
    pprint(accounts)

    for ad_account in accounts:
        record = FacebookAdAccountEntity.upsert(
            DEFAULT_SCOPE,
            ad_account['ad_account_id'],
            is_active=True,
            timezone=ad_account['timezone'],
            updated_by_sweep_id=job_scope.sweep_id
        )
        print(record)
