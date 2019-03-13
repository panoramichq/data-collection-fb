import logging
from facebook_business.adobjects import page

from facebook_business.adobjects.business import Business
from facebook_business.adobjects.page import Page
from facebook_business.api import FacebookRequest
from facebook_business.exceptions import FacebookError
from common.bugsnag import BugSnagContextData
from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.id_tools import generate_universal_id
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import (
    PlatformApiContext,
    FacebookApiErrorInspector,
    get_default_fields
)
from oozer.common.cold_storage.batch_store import NormalStore
from oozer.common.job_context import JobContext
from oozer.common.job_scope import JobScope
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.common.vendor_data import add_vendor_data
from oozer.reporting import reported_task

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@reported_task
def collect_page_task(job_scope: JobScope, job_context: JobContext) -> int:

    if not SweepRunningFlag.is_set(job_scope.sweep_id):
        logger.info(
            f'{job_scope} skipped because sweep {job_scope.sweep_id} is done'
        )
        return 0

    logger.info(
        f'{job_scope} started'
    )

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    collect_page(job_scope, job_context)
    return 1  # we collect 1 page at a time


def collect_page(job_scope: JobScope, _job_context: JobContext):
    """
    Collect a single facebook page
    """

    if job_scope.report_variant != Entity.Page:
        raise ValueError(
            f"Report level {job_scope.report_variant} specified is not: {Entity.Page}"
        )

    token = job_scope.token
    if not token:
        raise ValueError(
            f"Job {job_scope.job_id} cannot proceed. No platform tokens provided."
        )

    # We don't use it for getting a token. Something else that calls us does.
    # However, we use it to report usages of the token we got.
    token_manager = PlatformTokenManager.from_job_scope(job_scope)

    with PlatformApiContext(token) as fb_ctx:
        page_inst = page.Page(fbid=job_scope.entity_id, api=fb_ctx.api)
        page_fetched = page_inst.api_get(fields=get_default_fields(Page))
        report_job_status_task.delay(ExternalPlatformJobStatus.DataFetched, job_scope)
        token_manager.report_usage(token, 2)

        record_id_data = job_scope.to_dict()
        record_id_data.update(
            entity_type=Entity.Page,
            entity_id=job_scope.entity_id,
            report_variant=None,
        )
        entity_data = page_fetched.export_all_data()
        entity_data = add_vendor_data(
            entity_data,
            id=generate_universal_id(
                **record_id_data
            )
        )
        store = NormalStore(job_scope)
        store.store(entity_data)


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
@reported_task
def collect_pages_from_business_task(job_scope: JobScope, job_context: JobContext) -> int:
    """
        This task should import pages from FB using Business API. At the moment, it is not used anywhere.
    """

    if not SweepRunningFlag.is_set(job_scope.sweep_id):
        logger.info(
            f'{job_scope} skipped because sweep {job_scope.sweep_id} is done'
        )
        raise ConnectionError(Exception(f'{job_scope} skipped because sweep {job_scope.sweep_id} is done'), 0)

    logger.info(
        f'{job_scope} started'
    )

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    collect_pages_from_business(job_scope, job_context)


def collect_pages_from_business(job_scope: JobScope, _job_context: JobContext):
    """
    Collect all facebook pages that are active

    :param JobScope job_scope: The dict representation of JobScope
    :param JobContext _job_context:
    """

    report_job_status_task.delay(ExternalPlatformJobStatus.Start, job_scope)

    try:
        if job_scope.report_variant != Entity.Page:
            raise ValueError(
                f"Report level {job_scope.report_variant} specified is not: {Entity.Page}"
            )

        token = job_scope.token
        if not token:
            raise ValueError(
                f"Job {job_scope.job_id} cannot proceed. No platform tokens provided."
            )

        # We don't use it for getting a token. Something else that calls us does.
        # However, we use it to report usages of the token we got.
        token_manager = PlatformTokenManager.from_job_scope(job_scope)

    except Exception as ex:
        BugSnagContextData.notify(ex, job_scope=job_scope)

        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            ExternalPlatformJobStatus.GenericError, job_scope
        )
        raise

    try:
        with PlatformApiContext(token) as fb_ctx:
            fb_req = FacebookRequest(node_id="me", method="GET", endpoint="/businesses",
                                     api=fb_ctx.api, api_type='EDGE', target_class=Business)
            businesses = fb_req.execute()

        report_job_status_task.delay(ExternalPlatformJobStatus.DataFetched, job_scope)
        token_manager.report_usage(token)

        entity_type = Entity.Page

        record_id_base_data = job_scope.to_dict()
        record_id_base_data.update(
            entity_type=entity_type,
            report_variant=None,
        )

        for biz in businesses:
            client_pages = list(biz.get_client_pages(fields=get_default_fields(Page)))
            owned_pages = list(biz.get_owned_pages(fields=get_default_fields(Page)))
            pages_list = (client_pages + owned_pages)

            for page in pages_list:

                entity_data = page.export_all_data()
                record_id_base_data.update(
                    entity_id=entity_data.get('id')
                )
                entity_data = add_vendor_data(
                    entity_data,
                    id=generate_universal_id(
                        **record_id_base_data
                    )
                )

                store = NormalStore(job_scope)
                store.store(entity_data)

        report_job_status_task.delay(ExternalPlatformJobStatus.Done, job_scope)
        return businesses

    except FacebookError as e:
        # Build ourselves the error inspector
        inspector = FacebookApiErrorInspector(e)

        # Is this a throttling error?
        if inspector.is_throttling_exception():
            failure_status = ExternalPlatformJobStatus.ThrottlingError
            failure_bucket = FailureBucket.Throttling

        # Did we ask for too much data?
        elif inspector.is_too_large_data_exception():
            failure_status = ExternalPlatformJobStatus.TooMuchData
            failure_bucket = FailureBucket.TooLarge

        # It's something else which we don't understand
        else:
            failure_status = ExternalPlatformJobStatus.GenericPlatformError
            failure_bucket = FailureBucket.Other

        report_job_status_task.delay(failure_status, job_scope)
        token_manager.report_usage_per_failure_bucket(token, failure_bucket)
        raise

    except Exception as ex:
        BugSnagContextData.notify(ex, job_scope=job_scope)

        # This is a generic failure, which does not help us at all, so, we just
        # report it and bail
        report_job_status_task.delay(
            ExternalPlatformJobStatus.GenericError, job_scope
        )
        token_manager.report_usage_per_failure_bucket(token, FailureBucket.Other)
        raise
