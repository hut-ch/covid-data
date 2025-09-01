"""Standardised logging function"""

import logging
import sys


def get_logger(
    name: str, log_file: str = "pipeline.log", level=logging.INFO
) -> logging.Logger:
    """
    Returns a configured Logger with
    standardised formattring to stout and log file
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | \
%(name)s:%(funcName)s:%(lineno)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
