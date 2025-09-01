"""Manage UKspecific Facts"""

from utils import get_logger, get_variable

logger = get_logger(__name__)


def maintain_uk_facts(env_vars: dict | None):
    "Run EU Fact loads"

    logger.info("Starting UK Fact Load")

    # db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    logger.info("%s", target_schema)

    logger.info("Finished UK Fact Load")
