"""Manage EU specific Facts"""

from load.dw import maintain_table
from utils import get_db_engine, get_logger, get_variable

logger = get_logger(__name__)


def maintain_eu_facts(env_vars: dict | None):
    "Run EU Fact loads"

    logger.info("Starting EU Fact Load")

    db_engine = get_db_engine(env_vars)
    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    facts = [
        "fact_cases_deaths_country_daily",
        "fact_cases_deaths_country_weekly",
        "fact_movement_indicators_country",
        "fact_movement_indicators_region",
        "fact_vaccine_tracker_country",
        "fact_vaccine_tracker_region",
    ]
    for fact in facts:
        maintain_table(db_engine, fact, target_schema, "fact", "eu", env_vars)

    logger.info("Finished EU Fact Load")
