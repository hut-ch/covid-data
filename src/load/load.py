"""Runs all load processes"""

from load.dw import (
    create_dimensional_model_eu,
    drop_dimensional_model_eu,
    process_dimension,
)
from utils import get_db_engine, get_variable


def load_eu(env_vars: dict | None, refresh: bool = False):
    "Run EU loads"

    print("###################################")
    print("Starting EU Load")

    if refresh:
        drop_dimensional_model_eu(env_vars)

    create_dimensional_model_eu(env_vars)

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

    facts = [
        "fact_cases_deaths_country_daliy",
        "fact_cases_deaths_country_weekly",
        "fact_movement_indicators_country",
        "fact_movement_indicators_region",
        "fact_vaccinations_country",
        "fact_vaccinations_region",
    ]

    print("Processing Dimensions")
    for dim in dimensions:
        process_dimension(db_engine, dim, target_schema, env_vars)

    print("Processing Facts")
    for fact in facts:
        print(fact)

    print("\nFinished EU Load")


def load_uk(env_vars: dict | None):
    "Run UK Loads"

    print("###################################")
    print("Starting UK Load")

    print(get_variable("DB_HOST", env_vars))
    print(get_variable("DB_PORT", env_vars))
    print(get_variable("DB_USER", env_vars))
    print(get_variable("DB_DRIVER", env_vars))
    print(get_variable("DB_SCHEMA", env_vars))

    print("\nFinished UK Load")


def load_all(env_vars: dict | None):
    "Run all loads"
    load_eu(env_vars)
    load_uk(env_vars)
