"""Manage EU specific Dimensions"""

from utils import get_db_engine, get_logger, get_variable

from .manage_dimensions import process_dimension

logger = get_logger(__name__)


def maintain_eu_dims(env_vars: dict | None):
    "Run EU loads"

    logger.info("Starting EU Dimension Load")

    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    dimensions = [
        "dim_age",
        "dim_country",
        "dim_region",
        "dim_source",
        "dim_status",
        "dim_vaccine",
    ]

    for dim in dimensions:
        process_dimension(db_engine, dim, target_schema, env_vars)


def dim_age(env_vars: dict | None):
    """Maintain dim_age"""
    logger.info("Starting EU dim_age Load")
    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    process_dimension(db_engine, "dim_age", target_schema, env_vars)


def dim_country(env_vars: dict | None):
    """Maintain dim_country"""
    logger.info("Starting EU dim_country Load")
    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    process_dimension(db_engine, "dim_country", target_schema, env_vars)


def dim_region(env_vars: dict | None):
    """Maintain dim_country"""
    logger.info("Starting EU dim_region Load")
    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    process_dimension(db_engine, "dim_region", target_schema, env_vars)


def dim_source(env_vars: dict | None):
    """Maintain dim_country"""
    logger.info("Starting EU dim_source Load")
    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    process_dimension(db_engine, "dim_source", target_schema, env_vars)


def dim_status(env_vars: dict | None):
    """Maintain dim_country"""
    logger.info("Starting EU dim_status Load")
    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    process_dimension(db_engine, "dim_status", target_schema, env_vars)


def dim_vaccine(env_vars: dict | None):
    """Maintain dim_country"""
    logger.info("Starting EU dim_vaccine Load")
    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    process_dimension(db_engine, "dim_vaccine", target_schema, env_vars)
