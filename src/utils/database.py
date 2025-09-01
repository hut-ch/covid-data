"""main db util"""

import uuid
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, engine, text
from sqlalchemy.exc import (
    InternalError,
    NoSuchModuleError,
    OperationalError,
    ProgrammingError,
)

from utils.config import get_variable
from utils.data import check_columns_exist, get_unique_data, is_subset
from utils.logs import get_logger

logger = get_logger(__name__)


def get_db_engine(
    env_vars: dict | None, schema_name: Optional[str] = None
) -> Optional[engine.Engine] | None:
    """Create a db engine to be used by other functions

    Args:
        schema: Optional database schema name

    Returns:
        SQLAlchemy engine or None if connection fails
    """
    # get db credentials from environment file for a bit of security
    load_dotenv(".env")

    # get all the environmentt variable
    host = get_variable("DB_HOST", env_vars)
    port = get_variable("DB_PORT", env_vars)
    user = get_variable("DB_USER", env_vars)
    pw = get_variable("DB_PASSWORD", env_vars)
    db = get_variable("DB_DATABASE", env_vars)
    driver = get_variable("DB_DRIVER", env_vars)

    req_schema = (
        schema_name
        if schema_name is not None
        else (get_variable("DB_SCHEMA", env_vars) or "public")
    )

    if all([host, port, user, pw, db, driver]):
        db_url = f"{driver}://{user}:{pw}@{host}:{port}/{db}"
        db_url += f"?options=-csearch_path%3D{req_schema}"

        try:
            return create_engine(db_url)
        except ConnectionError as e:
            logger.error("Unable to access database: %s", repr(e))
        except NoSuchModuleError as e:
            logger.error("Invalid Driver: %s", repr(e))

    return None


def create_temp_table(
    db_engine: engine.Engine, data: pd.DataFrame, dest_schema: str
) -> str | None:
    """Create a temp table to hold data for the upsert"""
    temp_table = f"temp_{uuid.uuid4().hex[:10]}"

    logger.info("Creating temp table %s", temp_table)

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
        logger.error("Couldn't create temp table for update: %s", repr(e))
        return None


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
        logger.error("failed to check for table: %s", repr(e))
        return False


def check_data_exists(db_engine: engine.Engine, target_schema: str, dim: str) -> bool:
    """Check if data already exists in table to determine data insertion method"""
    with db_engine.connect() as con:
        data_check = con.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {target_schema}.{dim}
                WHERE {dim}_key <> -1;
                """  # nosec
            )
        ).first()[0]

    return data_check > 0


def run_query_script(file: str, env_vars: dict | None):
    """executes queries in given file"""

    # Open and read the file as a single buffer
    with open(file, "r", encoding="utf8") as f:
        sql_tables = f.read()

    queries = sql_tables.split("--#")

    # create connection to db
    db_engine = get_db_engine(env_vars)

    if db_engine is None:
        logger.error("Failed to create database engine")
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
                            result = con.execute(query)
                            logger.info("Query %s executed successfully: %s", i, result)
                    except (ProgrammingError, InternalError) as e:
                        logger.error("Error executing query %s: %s", i, repr(e))
                        logger.error("Query: %s", query)
        except OperationalError as e:
            logger.error("Database connection error: %s", str(e))


def get_other_cols(db_engine: engine.Engine, table_name: str, schema: str) -> list:
    """
    get other columns from the table that arent part of the unique ontraint

    this will be checkd against the dataframe and used to inser/merge the data
    """

    # Create bind parameters
    params = {"schema": schema, "table_name": table_name}

    query = text(
        """
    WITH key_cols AS (
        SELECT
            cu.column_name
        FROM
            information_schema.constraint_column_usage cu
            INNER JOIN information_schema.table_constraints tc
            ON cu.constraint_name = tc.constraint_name
        WHERE
            cu.table_name = :table_name
            AND tc.constraint_type IN ('UNIQUE','PRIMARY KEY')
            AND tc.table_schema = :schema
    )
    SELECT
        cols.column_name
    FROM
        information_schema.columns cols
        LEFT JOIN key_cols
            ON cols.column_name = key_cols.column_name
    WHERE
        table_schema = :schema
        AND table_name   = :table_name
        AND cols.column_name NOT IN ('created_ts','updated_ts')
        AND key_cols IS NULL
    """
    )

    with db_engine.connect() as con:
        cols = con.execute(query, params)

    return [col[0] for col in cols]


def get_unique_const_cols(
    db_engine: engine.Engine, table_name: str, schema: str
) -> list:
    """
    get columns from the unique constraint on the table

    this can them be used to ensure the dataframe's index is aligned
    and the data is unique
    """

    # Create bind parameters
    params = {"schema": schema, "table_name": table_name}

    query = text(
        """
    SELECT
        cu.column_name
    FROM
        information_schema.constraint_column_usage cu
        INNER JOIN information_schema.table_constraints tc
        ON cu.constraint_name = tc.constraint_name
    WHERE
        cu.table_name = :table_name
        AND tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = :schema
    """
    )

    with db_engine.connect() as con:
        cols = con.execute(query, params)

    return [col[0] for col in cols]


def validate_data_against_table(
    data: pd.DataFrame,
    db_engine: engine.Engine,
    table_name: str,
    schema: str,
    return_index: bool = False,
) -> tuple[pd.DataFrame | None, list | None, list | None, bool | None]:
    """Validate required columns from table exist in the dataframe

        Args:
        data: DataFrame to validate
        db_engine: SQLAlchemy engine
        table_name: Target table name
        schema: Database schema
        return_index: Whether to return index flag for inserts

    Returns:
        Tuple of (validated DataFrame, key columns, update columns, index flag)
        Index flag is only returned if return_index is True, else None
    """

    # get unique keys and other columns from table metadata
    keys = get_unique_const_cols(db_engine, table_name, schema)
    table_cols = get_other_cols(db_engine, table_name, schema)

    # ensure that unique keys exist in data and set the index to keys
    if is_subset(data.columns, keys):
        just_key = list(data.columns) == keys
        if not just_key:
            data = data.set_index(keys)
            insert_index = None if not return_index else True
        else:
            insert_index = None if not return_index else False

    else:
        logger.warning("Table keys %s not found in data %s", keys, data.columns)
        return None, None, None, None

    # check and return any non constraint columns that exist in the data
    cols = check_columns_exist(data, table_cols, warn=False)[0]

    if not cols:
        logger.info("Only %s found no other valid columns", keys)

    # de duplicate the data based on unique keys just as a
    # final safety check to ensure data can be inserted
    full_cols = keys + (cols if cols is not None else [])

    data = get_unique_data(data, full_cols, keys)

    return data, keys, cols, insert_index
