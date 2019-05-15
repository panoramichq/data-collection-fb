import logging

from common.celeryapp import get_celery_app
from common.error_inspector import ErrorInspector
from common.measurement import Measure
from oozer.common.job_scope import JobScope

logger = logging.getLogger(__name__)
app = get_celery_app()


def _extract_tags_for_report_job_status(stage_status: int, job_scope: JobScope, *_, **__):
    return {'entity_type': job_scope.entity_type, 'ad_account_id': job_scope.ad_account_id, 'stage_id': stage_status}


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_for_report_job_status)
@Measure.counter(
    __name__,
    function_name_as_metric=True,
    count_once=True,
    extract_tags_from_arguments=_extract_tags_for_report_job_status,
)
def report_job_status_task(stage_status: int, job_scope: JobScope):
    """
    We take job scope to divine basic information about the job itself.

    Stage id gives us the number we will be reporting.

    And status_context is some sort of dictionary that allows us to divine
    failure contexts
    """
    from oozer.common.report_job_status import report_job_status
    from pynamodb.exceptions import PutError

    try:
        report_job_status(stage_status, job_scope)
    except PutError as ex:
        if ErrorInspector.is_dynamo_throughput_error(ex):
            logger.info(str(ex))
        else:
            raise
