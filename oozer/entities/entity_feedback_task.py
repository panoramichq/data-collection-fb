from common.celeryapp import get_celery_app

app = get_celery_app()


@app.task
def entity_feedback_task(entity_data, entity_type, entity_hash_pair):
    """
    This task is to feedback information about entity collected by updating
    data store.

    :param dict entity_data: The entity we're feeding back to the system
    :param string entity_type: Type of the entity, a string representation
    :param tuple(string, string) entity_hash_pair: Tuple containing both entity data
        itself and fields hashes that we can use

    """
    from .entity_feedback import entity_feedback
    entity_feedback(entity_data, entity_type, entity_hash_pair)
