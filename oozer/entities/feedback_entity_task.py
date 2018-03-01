from common.celeryapp import get_celery_app


app = get_celery_app()


@app.task
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
    feedback_entity(entity_data, entity_type, entity_hash_pair)
