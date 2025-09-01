"""Runs all transformations for EU data"""

from transform.eu import mi_transform, nd_transform, vt_transform
from transform.uk import uk_placeholder
from utils import get_logger

logger = get_logger(__name__)


def transform_all(env_vars: dict | None):
    "Run all transformations"

    transform_eu(env_vars)
    transform_uk(env_vars)


def transform_eu(env_vars: dict | None):
    "Run EU transformations"

    logger.info("Starting EU Transformation")

    mi_transform(env_vars)
    nd_transform(env_vars)
    vt_transform(env_vars)

    logger.info("Finished EU Transformation")


def transform_uk(env_vars: dict | None):
    "Run UK transformations"

    logger.info("Starting UK Transformation")

    uk_placeholder(env_vars)

    logger.info("Finished UK Transformation")
