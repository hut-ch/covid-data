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
    check_data_exists,
    check_table_exists,
    create_temp_table,
    file_check,
    get_dir,
    import_transformed_data,
    validate_data_against_table,
)


def insert_data(
    db_engine: engine.Engine, data: pd.DataFrame, table_name: str, schema: str
):
    """Insert data into target table"""

    if check_table_exists(db_engine, table_name, schema):
        # Check data is valid against table and cleanup data

        valid_data, _, _, insert_index = validate_data_against_table(
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
    valid_data = validate_data_against_table(
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
            # con.commit()
            print(f"table {table_name} - {result.rowcount} rows updated")
    except DBAPIError as e:
        print("Merge failed ", e)
        with db_engine.connect() as con:
            con.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
            # con.commit()
        return


def process_dimension(
    db_engine: engine.Engine, dim: str, schema: str, env_vars: dict | None
):
    """load data into dimension checking if data exists and merging accordingly"""
    print(f"Processing {dim}")
    if check_table_exists(db_engine, dim, schema):
        file_path = get_dir("CLEANSED_FOLDER", "eu", env_vars)
        file_search = dim[4:]  # remove dim_
        dim_files = file_check(file_path, f"/*{file_search}*.json")

        if dim_files is None:
            print(f"no files found for {dim}")
            return

        for file in dim_files:
            print(f"processing {file}")

            data = import_transformed_data(file)

            if data is None:
                return

            if check_data_exists(db_engine, schema, dim):
                print("upsert")
                upsert_data(db_engine, data, dim, schema)
            else:
                print("insert")
                insert_data(db_engine, data, dim, schema)
