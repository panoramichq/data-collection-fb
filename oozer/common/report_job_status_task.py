import logging

from common.celeryapp import get_celery_app
from common.measurement import Measure
from oozer.common.job_scope import JobScope

logger = logging.getLogger(__name__)
app = get_celery_app()


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def report_job_status_task(stage_status: int, job_scope: JobScope):
    """
    We take job scope to divine basic information about the job itself.

    Stage id gives us the number we will be reporting.

    And status_context is some sort of dictionary that allows us to divine
    failure contexts
    """
    from .report_job_status import report_job_status
    from pynamodb.exceptions import PutError

    try:
        report_job_status(stage_status, job_scope)
    except PutError as ex:
        # pynamodb.exceptions.PutError: Failed to put item: An error occurred (ProvisionedThroughputExceededException) on request (6P1M2OO24PI2RJ6ALBBSDROE8RVV4KQNSO5AEMVJF66Q9ASUAAJG) on table (1c5595-datacol-APP_DYNAMODB_FB_SWEEP_ENTITY_REPORT_TYPE_TABLE) when calling the PutItem operation: The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API
        # a particular noisy type of this error - ProvisionedThroughputExceededException
        # may happen a lot in prod. At some point Write Units on the table
        # will auto-scale and the error will go away.
        # So, not allowing this to noisy up our logs much
        ex_str = str(ex)
        if 'ProvisionedThroughputExceededException' in ex_str:
            logger.info(ex_str)
        else:
            # rest is ok to bubble up
            raise
