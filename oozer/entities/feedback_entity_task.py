import logging
from typing import Any, Dict
from pynamodb.exceptions import PutError, UpdateError

from common.error_inspector import ErrorInspector
from oozer.entities.feedback_entity import feedback_entity, determine_ad_account_id
from common.celeryapp import get_celery_app
from common.measurement import Measure

logger = logging.getLogger(__name__)
app = get_celery_app()


def _extract_tags_for_feedback_entity(entity_data: Dict[str, Any], entity_type: str, *__, **___):
    return {'entity_type': entity_type, 'ad_account_id': determine_ad_account_id(entity_data, entity_type)}


@app.task
@Measure.timer(__name__, function_name_as_metric=True, extract_tags_from_arguments=_extract_tags_for_feedback_entity)
@Measure.counter(
    __name__,
    function_name_as_metric=True,
    count_once=True,
    extract_tags_from_arguments=_extract_tags_for_feedback_entity,
)
def feedback_entity_task(entity_data: Dict[str, Any], entity_type: str):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param entity_data: The entity we're feeding back to the system
    :param entity_type: Type of the entity, a string representation
    """

    try:
        feedback_entity(entity_data, entity_type)
    except (PutError, UpdateError) as ex:
        if ErrorInspector.is_dynamo_throughput_error(ex):
            Measure.counter(
                feedback_entity_task.__name__ + '.throughput_exceptions',
                tags={'entity_type': entity_type, 'ad_account_id': determine_ad_account_id(entity_data, entity_type)},
            ).increment()
            logger.info(str(ex))
        else:
            raise
