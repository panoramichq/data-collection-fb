import logging
from typing import Any, Dict

from common.celeryapp import get_celery_app
from common.measurement import Measure

logger = logging.getLogger(__name__)
app = get_celery_app()


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def feedback_entity_task(entity_data: Dict[str, Any], entity_type: str, entity_hash_pair):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param entity_data: The entity we're feeding back to the system
    :param entity_type: Type of the entity, a string representation
    :param entity_hash_pair: Tuple containing both entity data
        itself and fields hashes that we can use
    """
    from oozer.entities.feedback_entity import feedback_entity
    from pynamodb.exceptions import PutError, UpdateError

    try:
        feedback_entity(entity_data, entity_type, entity_hash_pair)
    except (PutError, UpdateError) as ex:
        ex_str = str(ex)
        if 'ProvisionedThroughputExceededException' in ex_str:
            Measure.counter(
                feedback_entity_task.__name__ + '.throughput_exceptions',
                tags={'entity_type': entity_type, 'ad_account_id': entity_data['account_id']},
            ).increment()
            logger.warning(ex_str)
        else:
            # rest is ok to bubble up
            raise
