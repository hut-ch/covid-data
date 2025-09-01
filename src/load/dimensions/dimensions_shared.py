"""Manage EU specific Dimensions"""

from utils import get_logger, get_variable

# from .manage_dimensions import process_dimension

logger = get_logger(__name__)


def maintain_shared_dims(env_vars: dict | None):
    "Run EU loads"

    logger.info("Starting Shared Dimension Load")

    dim_date(env_vars)


def dim_date(env_vars: dict | None):
    """Maintain dim_date"""
    logger.info("Starting dim_date Load")
    # db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    logger.info("%s", target_schema)

    # process_dimension(db_engine, "dim_date", target_schema, env_vars)
