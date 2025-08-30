"""update"""

from sqlalchemy import engine, schema, text
from sqlalchemy.exc import InternalError, ProgrammingError

from utils import file_exists, get_db_engine, get_variable, run_query_script


def drop_dimensional_model_eu(env_vars: dict | None):
    """run drop tables script EU"""

    if file_exists("./src/sql/", "drop_tables_eu.sql"):
        print("Droping Tables")
        run_query_script("./src/sql/drop_tables_eu.sql", env_vars)
    else:
        print("Skipping Table Drop - No file")


def create_dimensional_model_eu(env_vars: dict | None):
    """run create tables script EU"""

    if file_exists("./src/sql/", "create_tables_eu.sql"):
        print("Creating Tables")
        run_query_script("./src/sql/create_tables_eu.sql", env_vars)
    else:
        print("Skipping Table Creation - No file")


def check_schema_exists(db_engine: engine.Engine, target_schema: str) -> bool:
    """check table exists in db"""

    # Create bind parameters
    params = {"schema": target_schema}

    query = text(
        """
            SELECT EXISTS(
                SELECT
                FROM information_schema.schemata
                WHERE
                    schema_name = :schema
                );
            """
    )

    try:
        with db_engine.connect() as con:
            return bool(con.execute(query, params).first()[0])
    except (InternalError, ProgrammingError) as e:
        print("failed to check for schema", e)
        return False


def create_schema(env_vars: dict | None):
    """Creates a new schema if it doesn't exist"""

    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"

    db_engine = get_db_engine(env_vars=env_vars, schema_name="public")

    if not db_engine:
        print("Failed to create database engine")
        return

    if not check_schema_exists(db_engine, target_schema):
        try:
            with db_engine.connect() as con:
                con.execute(schema.CreateSchema(name=target_schema, if_not_exists=True))
        except (InternalError, ProgrammingError) as e:
            print("Failed to create schema", e)
            print("Failed to create schema", e)
            print("Failed to create schema", e)
