import logging

def get_logger(name=__name__, level="INFO"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)

        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
