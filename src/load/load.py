"""Runs all load processes"""

from load.dimensions import maintain_eu_dims, maintain_shared_dims, maintain_uk_dims
from load.dw import create_dimensional_model_eu, drop_dimensional_model_eu
from load.facts import maintain_eu_facts, maintain_uk_facts
from utils import get_logger

logger = get_logger(__name__)


def load_eu(env_vars: dict | None, refresh: bool = False):
    "Run EU loads"

    logger.info("Starting EU Load")

    if refresh:
        drop_dimensional_model_eu(env_vars)

    create_dimensional_model_eu(env_vars)

    logger.info("Processing Dimensions")

    maintain_eu_dims(env_vars)

    logger.info("Processing Facts")

    maintain_eu_facts(env_vars)

    logger.info("Finished EU Load")


def load_uk(env_vars: dict | None):
    "Run UK Loads"

    logger.info("Starting UK Load")

    maintain_uk_dims()
    maintain_uk_facts(env_vars)

    logger.info("Finished UK Load")


def load_shared(env_vars: dict | None):
    "Run Shareed Loads"

    logger.info("Starting Shared Load")
    maintain_shared_dims(env_vars)

    logger.info("Finished Shared Load")


def load_all(env_vars: dict | None):
    "Run all loads"
    load_shared(env_vars)
    load_eu(env_vars)
    load_uk(env_vars)
