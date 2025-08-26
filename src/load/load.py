"""Runs all load processes"""

import os

import pandas as pd
from sqlalchemy import engine, text

from load.dw import insert_data, upsert_data
from utils import (
    check_table_exists,
    create_schema,
    file_check,
    file_exists,
    get_db_engine,
    get_dir,
    run_query_script,
)


def drop_dimensional_model_eu():
    """run drop tables script EU"""

    if file_exists("./src/sql/", "drop_tables_eu.sql"):
        run_query_script("./src/sql/drop_tables_eu.sql")
    else:
        print("Skipping Schema Drop - NO file")


def create_dimensional_model_eu():
    """run create tables script EU"""

    create_schema()

    if file_exists("./src/sql/", "create_tables_eu.sql"):
        run_query_script("./src/sql/create_tables_eu.sql")
    else:
        print("Skipping Schema Creation - NO file")


def check_data_already_exists(db_engine: engine.Engine, schema: str, dim: str) -> bool:
    """Check if data already exists in table to determine data insertion method"""
    with db_engine.connect() as con:
        data_check = con.execute(
            text(f"SELECT COUNT(*) FROM {schema}.{dim} WHERE {dim}_key <> -1")  # nosec
        ).first()[0]

    return data_check > 0


def import_data(file: str) -> pd.DataFrame | None:
    """try loading the json file depending on format"""
    try:
        data = pd.read_json(file)
        return data
    except ValueError:  # as e:
        # print("couldn't load file re attempting in different format", repr(e))
        pass

    try:
        data = pd.read_json(file, lines=True)
        return data
    except ValueError as e:
        print(f"unable to load data from file {file}", repr(e))
        return None


def load_eu(refresh: bool = False):
    "Run EU loads"

    print("###################################")
    print("Starting EU Load")

    if refresh:
        drop_dimensional_model_eu()

    create_dimensional_model_eu()

    db_engine = get_db_engine()
    schema = os.getenv("DB_SCHEMA", "public")

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
        print(f"Processing {dim}")
        if check_table_exists(db_engine, dim, schema):
            file_path = get_dir("CLEANSED_FOLDER", "eu")
            file_search = dim[4:]  # remove dim_
            dim_files = file_check(file_path, f"/*{file_search}*.json")

            if dim_files is None:
                print(f"no files found for {dim}")
                continue

            for file in dim_files:
                print(f"processing {file}")

                data = import_data(file)

                if data is None:
                    continue

                if check_data_already_exists(db_engine, schema, dim):
                    print("upsert")
                    upsert_data(db_engine, data, dim, schema)
                else:
                    print("insert")
                    insert_data(db_engine, data, dim, schema)

    print("Processing Facts")
    for fact in facts:
        print(fact)

    print("\nFinished EU Load")


def load_uk():
    "Run UK Loads"

    print("###################################")
    print("Starting UK Load")

    print("\nFinished UK Load")


def load_all():
    "Run all loads"
    load_eu()
    load_uk()
