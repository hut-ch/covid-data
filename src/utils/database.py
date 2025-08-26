"""main db util"""

import os
import uuid
from typing import Optional

import dotenv
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, engine, schema, text
from sqlalchemy.exc import (
    InternalError,
    NoSuchModuleError,
    OperationalError,
    ProgrammingError,
)


def get_db_engine(schema_name: Optional[str] = None) -> Optional[engine.Engine] | None:
    """Create a db engine to be used by other functions

    Args:
        schema: Optional database schema name

    Returns:
        SQLAlchemy engine or None if connection fails
    """
    # get db credentials from environment file for a bit of security
    load_dotenv(".env")

    # get all the environmentt variable
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    pw = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_DATABASE")
    driver = os.getenv("DB_DRIVER")

    req_schema = (
        schema_name if schema_name is not None else os.getenv("DB_SCHEMA", "public")
    )

    if all([host, port, user, pw, db, driver]):
        db_url = f"{driver}://{user}:{pw}@{host}:{port}/{db}"
        db_url += f"?options=-csearch_path%3D{req_schema}"

        try:
            return create_engine(db_url)
        except ConnectionError as e:
            print("Unable to access database", repr(e))
        except NoSuchModuleError as e:
            print("Invalid Driver", repr(e))

    return None


def run_query_script(file: str):
    """executes queries in given file"""

    # Open and read the file as a single buffer
    with open(file, "r", encoding="utf8") as f:
        sql_tables = f.read()

    queries = sql_tables.split("--#")

    # create connection to db
    db_engine = get_db_engine()

    if db_engine is None:
        print("Failed to create database engine")
        return

    # Use a single connection for all queries
    with db_engine.connect() as con:
        try:
            for i, query in enumerate(queries, 1):
                if query.strip():  # Skip empty queries
                    try:
                        query = text(query)
                        # Begin a new transaction for each query
                        with con.begin():
                            con.execute(query)
                            print(f"Query {i} executed successfully")
                    except (ProgrammingError, InternalError) as e:
                        print(f"Error executing query {i}:", str(e))
                        print("Query:", query)
        except OperationalError as e:
            print("Database connection error:", str(e))


def create_temp_table(
    db_engine: engine.Engine, data: pd.DataFrame, dest_schema: str
) -> str | None:
    """Create a temp table to hold data for the upsert"""
    temp_table = f"temp_{uuid.uuid4().hex[:10]}"

    try:
        with db_engine.connect() as con:
            # output dataframe to temporary table ready for merge
            data.to_sql(
                temp_table,
                con,
                schema=dest_schema,
                index=True,
                if_exists="replace",
            )
        return temp_table
    except (ProgrammingError, InternalError) as e:
        print("Couldn't create temp table for Update", e)
        return None


def create_schema():
    """Creates a new schema if it doesn't exist"""

    dotenv.load_dotenv()
    target_schema = os.getenv("DB_SCHEMA", "public")

    db_engine = get_db_engine(schema_name="public")

    if not db_engine:
        print("Failed to create database engine")
        return

    try:
        with db_engine.connect() as con:
            con.execute(schema.CreateSchema(name=target_schema, if_not_exists=True))
            con.commit()

    except (InternalError, ProgrammingError) as e:
        print("Failed to create schema", e)


def check_table_exists(
    db_engine: engine.Engine, table_name: str, table_schema: str
) -> bool:
    """check table exists in db"""

    # Create bind parameters
    params = {"schema": table_schema, "table_name": table_name}

    query = text(
        """
            SELECT EXISTS(
                SELECT
                FROM information_schema.tables
                WHERE
                    table_schema = :schema
                    AND table_name = :table_name
                );
            """
    )

    try:
        with db_engine.connect() as con:
            return bool(con.execute(query, params).first()[0])
    except (InternalError, ProgrammingError) as e:
        print("failed to check for table", e)
        return False
