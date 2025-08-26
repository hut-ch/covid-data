"""test"""

import pandas as pd
from sqlalchemy import engine, text
from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)

from utils import (
    check_columns_exist,
    check_table_exists,
    create_temp_table,
    get_unique_data,
    is_subset,
)


def validate_data_againt_table(
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
        print(f"Table keys {keys} not found in data {data.columns}")
        return None, None, None, None

    # check and return any non constraint columns that exist in the data
    cols = check_columns_exist(data, table_cols, warn=False)[0]

    if not cols:
        print(f"Only {keys} found no other valid columns")

    # de duplicate the data based on unique keys just as a
    # final safety check to ensure data can be inserted
    full_cols = keys + (cols if cols is not None else [])

    data = get_unique_data(data, full_cols, keys)

    return data, keys, cols, insert_index


def insert_data(
    db_engine: engine.Engine, data: pd.DataFrame, table_name: str, schema: str
):
    """Insert data into target table"""

    if check_table_exists(db_engine, table_name, schema):
        # Check data is valid against table and cleanup data

        valid_data, _, _, insert_index = validate_data_againt_table(
            data,
            db_engine,
            table_name,
            schema,
            return_index=True,  # We want the index flag for inserts
        )

        if valid_data is None:
            print("Data did not pass validation checks skipping")
            return

        with db_engine.connect() as connection:
            try:
                inserted_rows = valid_data.to_sql(
                    name=table_name,
                    con=connection,
                    schema=schema,
                    if_exists="append",
                    chunksize=10000,
                    method="multi",
                    index=insert_index,
                )
                print(f"{table_name} - {inserted_rows} rows inserted")
            except OperationalError as e:
                print("Database connection error:", repr(e))
            except (ProgrammingError, IntegrityError) as e:
                print("Error inserting Data: ", repr(e))
    else:
        print(f"table {table_name} does not exist! Ensure table is in create script")


def create_merge_query(
    schema: str, source: str, target: str, keys: list, cols: list
) -> str:
    """contruct the merge query from components"""
    source_table = schema + "." + source
    target_table = schema + "." + target
    merge_conditions = " AND ".join([f"src.{key} = tgt.{key}" for key in keys])
    updates = ", ".join([f"{col} = src.{col}" for col in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"src.{col}" for col in cols])

    query = text(
        """
    MERGE INTO """  # nosec
        + target_table  # nosec
        + """ tgt
    USING """  # nosec
        + source_table  # nosec
        + """ src
    ON """  # nosec
        + merge_conditions  # nosec
        + """
    WHEN MATCHED THEN
        UPDATE SET
            """  # nosec
        + updates  # nosec
        + """,
            updated_ts = default
    WHEN NOT MATCHED THEN
        INSERT
            ("""  # nosec
        + insert_cols  # nosec
        + """)
        VALUES
            ("""  # nosec
        + insert_vals  # nosec
        + """)
    """
    )  # nosec

    return query


def upsert_data(
    db_engine: engine.Engine, data: pd.DataFrame, table_name: str, schema: str
):
    """Upsert/Merge data into target table"""

    if not check_table_exists(db_engine, table_name, schema):
        print(f"Table {table_name} does not exist! Ensure table is in create script")
        return

    # Check data is valid against table and cleanup data
    valid_data = validate_data_againt_table(
        data,
        db_engine,
        table_name,
        schema,
        return_index=False,  # We don't need the index flag for upserts
    )

    if any(x is None for x in valid_data[:2]):
        print("Data did not pass validation checks skipping")
        return

    update_data, keys, update_cols, _ = valid_data
    update_cols = update_cols or []  # Convert None to empty list
    keys = keys or []

    # Create temp table to hold data ahead or merge
    temp_table = create_temp_table(db_engine, update_data, schema)
    if temp_table is None:
        return

    # generate query
    all_cols = keys + update_cols
    query = create_merge_query(schema, temp_table, table_name, keys, all_cols)

    try:
        with db_engine.connect() as con:
            # perform the merge
            result = con.execute(query)
            con.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
            con.commit()
            print(f"table {table_name} - {result.rowcount} rows updated")
    except DBAPIError as e:
        print("Merge failed ", e)
        with db_engine.connect() as con:
            con.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
            con.commit()
        return


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
