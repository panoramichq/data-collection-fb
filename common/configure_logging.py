import logging

from config.logging import LEVEL

def configure_logging(level=LEVEL):
    logger = logging.getLogger()  # root logger
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    logger.addHandler(handler)
