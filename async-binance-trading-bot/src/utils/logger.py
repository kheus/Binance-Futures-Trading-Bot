from rich.logging import RichHandler
import logging
import os

def setup_logger(name):
    log_level = logging.DEBUG if os.getenv("DEBUG_MODE") == "true" else logging.INFO
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    handler = RichHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger

logger = setup_logger(__name__)