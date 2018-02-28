import logging

from config.logging import LEVEL

def configure_logging(level=LEVEL):
    logger = logging.getLogger()  # root logger
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            # %(pathname)s
            '%(relativeCreated)d  [%(name)s.%(funcName)s:%(lineno)d] %(message)s'
        )
    )
    logger.addHandler(handler)

    to_mute = [
        'botocore',
        'celery.app.trace',
        'celery.worker.strategy',
        'pynamodb',
    ]
    for module in to_mute:
        logging.getLogger(module).setLevel(logging.WARNING)
