import logging

from pynamodb.exceptions import UpdateError

from common.error_inspector import ErrorInspector
from common.measurement import Measure
from common.celeryapp import get_celery_app
from common.store.entities import ENTITY_TYPE_MODEL_MAP
from oozer.common.job_scope import JobScope

logger = logging.getLogger(__name__)
app = get_celery_app()


def _extract_tags_for_report_job_status(job_scope: JobScope, *_, **__):
    return {'entity_type': job_scope.entity_type, 'ad_account_id': job_scope.ad_account_id}


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_for_report_job_status)
@Measure.counter(
    __name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_for_report_job_status
)
def set_inaccessible_entity_task(job_scope: JobScope):
    """Update entity record to flag it as inaccessible."""
    entity_type = job_scope.entity_type
    entity_id = job_scope.entity_id

    model_factory = ENTITY_TYPE_MODEL_MAP[entity_type]
    model = model_factory(page_id=job_scope.ad_account_id, entity_id=entity_id)
    try:
        model.update(actions=[model_factory.is_accessible.set(False)])
    except UpdateError as ex:
        if ErrorInspector.is_dynamo_throughput_error(ex):
            logger.info(str(ex))
        else:
            raise
