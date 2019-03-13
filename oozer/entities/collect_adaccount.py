import logging

from facebook_business.exceptions import FacebookError

from common.bugsnag import BugSnagContextData
from common.celeryapp import get_celery_app
from common.enums.entity import Entity
from common.enums.failure_bucket import FailureBucket
from common.id_tools import generate_universal_id
from common.measurement import Measure
from common.tokens import PlatformTokenManager
from oozer.common.cold_storage.batch_store import NormalStore
from oozer.common.report_job_status_task import report_job_status_task
from oozer.common.enum import ExternalPlatformJobStatus
from oozer.common.facebook_api import PlatformApiContext, get_default_fields, FacebookApiErrorInspector
from oozer.common.job_scope import JobScope
from oozer.common.sweep_running_flag import SweepRunningFlag
from oozer.common.vendor_data import add_vendor_data
from oozer.entities.feedback_entity_task import feedback_entity_task

app = get_celery_app()
logger = logging.getLogger(__name__)


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def collect_adaccount_task(job_scope, job_context):
    # An early exit flag to kill off tasks before start when needed.
    # See the explanation in the entities per adaccount task
    if not SweepRunningFlag.is_set(job_scope.sweep_id):
        logger.info(
            f'{job_scope} skipped because sweep {job_scope.sweep_id} is done'
        )
        return

    logger.info(
        f'{job_scope} started'
    )

    if not job_scope.tokens:
        good_token = PlatformTokenManager.from_job_scope(job_scope).get_best_token()
        if good_token is not None:
            job_scope.tokens = [good_token]

    collect_adaccount(job_scope, job_context)


def collect_adaccount(job_scope, _job_context):
    """
    Collects ad account data for a AA specific JobScope definition

    :param JobScope job_scope: The JobScope as we get it from the task itself
    :param JobContext _job_context: A job context we use for entity checksums (not used at the moment)
    :rtype: Dict
    """

    report_job_status_task.delay(ExternalPlatformJobStatus.Start, job_scope)

    try:
        if job_scope.report_variant != Entity.AdAccount:
            raise ValueError(
                f"Report level {job_scope.report_variant} specified is not: {Entity.AdAccount}"
            )

        token = job_scope.token
        if not token:
            raise ValueError(
                f"Job {job_scope.job_id} cannot proceed. No platform tokens provided."
            )

        assert job_scope.ad_account_id == job_scope.entity_id, f'This is an ad account entity job, account_id should be equal to entity_id'

        # Used to report token usage by this job
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
            ad_account = fb_ctx.to_fb_model(
                job_scope.ad_account_id,
                Entity.AdAccount
            )

            fields = get_default_fields(ad_account.__class__)

            ad_account_with_selected_fields = ad_account.remote_read(fields=fields)  # Read just the fields we need
            ad_account_data_dict = ad_account_with_selected_fields.export_all_data()  # Export the object to a dict

            report_job_status_task.delay(ExternalPlatformJobStatus.DataFetched, job_scope)
            token_manager.report_usage(token)

            job_scope_base = dict(
                # Duplicate the job_scope data to avoid mutating it
                **job_scope.to_dict(),
            )
            job_scope_base.update(
                # Augment the job scpecific job scope fields so that it represents a single ad account
                # for the universal id construction
                entity_type=Entity.AdAccount,
                report_variant=None,
            )

            augmented_ad_account_data = add_vendor_data(
                # Augment the data returned from the remote API with our vendor data
                ad_account_data_dict,
                id=generate_universal_id(
                    **job_scope_base
                )
            )
            feedback_entity_task.delay(ad_account_data_dict, job_scope.report_variant, [None, None])
            store = NormalStore(job_scope)
            store.store(augmented_ad_account_data)

            # TODO: feedback account? this probably wouldn't make sense at the moment
            # because ad accounts are discovered from console and their lifecycle is controlled from there.

            report_job_status_task.delay(ExternalPlatformJobStatus.Done, job_scope)
            return ad_account_data_dict

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
