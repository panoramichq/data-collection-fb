import logging

from pynamodb.exceptions import PutError

from common.bugsnag import BugSnagContextData
from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from common.store.entities import PageEntity
from common.store.scope import DEFAULT_SCOPE
from common.tokens import PlatformTokenManager
from oozer.common.enum import JobStatus
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.scraper_api import ScraperApi


app = get_celery_app()
logger = logging.getLogger(__name__)

scope_api_map = {
    DEFAULT_SCOPE: ScraperApi
}

@app.task
def import_pages_task(job_scope, job_context):
    """
    Collect all facebook pages from the scraper API

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
            raise ValueError(f'No registered Page extractor API for scope "{job_scope.entity_id}"')
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
        pages = api.get_pages()
        for page in pages:
            try:
                PageEntity.upsert(
                    job_scope.entity_id,  # scope ID
                    page['id'],
                    updated_by_sweep_id=job_scope.sweep_id
                )

            except PutError as ex:
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
