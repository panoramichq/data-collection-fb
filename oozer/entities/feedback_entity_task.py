import logging

from common.celeryapp import get_celery_app
from common.measurement import Measure

logger = logging.getLogger(__name__)
app = get_celery_app()


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def feedback_entity_task(entity_data, entity_type, entity_hash_pair):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param dict entity_data: The entity we're feeding back to the system
    :param string entity_type: Type of the entity, a string representation
    :param tuple(string, string) entity_hash_pair: Tuple containing both entity data
        itself and fields hashes that we can use

    """
    from .feedback_entity import feedback_entity
    from pynamodb.exceptions import PutError, UpdateError

    try:
        feedback_entity(entity_data, entity_type, entity_hash_pair)
    except (PutError, UpdateError) as ex:
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
