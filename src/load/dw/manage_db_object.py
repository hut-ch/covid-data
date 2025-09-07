"""Functions for managing dimension tables in the database"""

import pandas as pd
from sqlalchemy import engine, text
from sqlalchemy.exc import (
    DataError,
    DBAPIError,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)

from utils import (  # merge_rows,
    check_data_exists,
    check_table_exists,
    create_temp_table,
    file_check,
    get_dir,
    get_foreign_key,
    get_logger,
    get_primary_key,
    get_unique_const_cols,
    import_transformed_data,
    is_subset,
    validate_data_against_table,
)

logger = get_logger(__name__)


def insert_data(
    db_engine: engine.Engine, data: pd.DataFrame, table_name: str, schema: str
):
    """Insert data into target table"""
    logger.info("Inserting data as %s is blank", table_name)

    if check_table_exists(db_engine, table_name, schema):
        # Check data is valid against table and cleanup data

        logger.info("Running validation checks on data")
        valid_data, _, _, insert_index = validate_data_against_table(
            data,
            db_engine,
            table_name,
            schema,
            return_index=True,  # We want the index flag for inserts
        )

        if valid_data is None:
            logger.warning("Data did not pass validation checks skipping")
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
                logger.info("%s - %s rows inserted", table_name, inserted_rows)
            except OperationalError as e:
                logger.error("Database connection error: %s", repr(e))
            except (ProgrammingError, IntegrityError, ValueError, DataError) as e:
                logger.error("Error inserting Data: %s", repr(e))
    else:
        logger.error(
            "Table %s does not exist! Ensure table is in create script", table_name
        )


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

    logger.info("Generating query for %s merge", target_table)

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
    ; COMMIT;"""
    )  # nosec

    return query


def upsert_data(
    db_engine: engine.Engine, data: pd.DataFrame, table_name: str, schema: str
):
    """Upsert/Merge data into target table"""

    # Check data is valid against table and cleanup data
    valid_data = validate_data_against_table(
        data,
        db_engine,
        table_name,
        schema,
        return_index=False,  # We don't need the index flag for upserts
    )

    if any(x is None for x in valid_data[:2]):
        logger.warning("Data did not pass validation checks skipping")
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

            logger.info("table %s - %s rows updated", table_name, result.rowcount)
    except DBAPIError as e:
        logger.error("Merge failed %s", e)
        with db_engine.connect() as con:
            con.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))

        return


def append_dimension_key(
    db_engine: engine.Engine, data: pd.DataFrame, dim: str, schema: str
) -> pd.DataFrame:
    """Add input dimensions key to source Dataframe by matching
    on unique contraint columns defined in the dimension"""

    dimension_keys = get_primary_key(db_engine, dim[1], schema)
    business_keys = get_unique_const_cols(db_engine, dim[1], schema)

    dim_default = {key: -1 for key in dimension_keys}

    if dim[1] == "dim_date":
        with db_engine.connect() as con:
            dim_data = pd.read_sql(
                con=con, sql=dim[1], columns=dimension_keys + business_keys
            )

        left = str(dim[0]).replace("dim_", "").replace("_key", "")

        dim_data["date"] = dim_data["date"].astype("int64")
        data[left] = data[left].astype("int64")

        logger.info("Getting key for: %s from %s", left, dim[1])

        merged_data = pd.merge(
            data,
            dim_data,
            how="left",
            left_on=left,
            right_on=business_keys,
        ).fillna(dim_default)

        merged_data.rename(
            columns={"dim_date_key": dim[0]},
            inplace=True,
        )

    elif is_subset(data.columns, business_keys):
        with db_engine.connect() as con:
            dim_data = pd.read_sql(
                con=con, sql=dim[1], columns=dimension_keys + business_keys
            )

        merged_data = pd.merge(
            data,
            dim_data,
            how="left",
            on=business_keys,
        ).fillna(dim_default)

        if "dim_age_key" in merged_data.columns:
            merged_data["dim_age_key"] = merged_data["dim_age_key"].astype("int64")
    else:
        logger.warning(
            "Business key: %s missing from data, setting %s to default -1",
            business_keys,
            dim,
        )
        key_name = str(*dimension_keys)
        data[key_name] = -1
        merged_data = data

    return merged_data


def maintain_table(
    db_engine: engine.Engine,
    table_name: str,
    target_schema: str,
    table_type: str,
    folder: str,
    env_vars: dict | None,
):
    """load data into dimension checking if data exists and merging accordingly"""
    logger.info("Processing %s", table_name)
    if check_table_exists(db_engine, table_name, target_schema):
        file_path = get_dir("CLEANSED_FOLDER", folder, env_vars)

        table_files = file_check(file_path, f"/*{table_name}*.json")

        if table_files is None:
            logger.warning("No files found for %s", table_name)
            return

        for file in table_files:
            logger.info("Processing %s", file)

            data = import_transformed_data(file)

            if data is None:
                return

            if table_type == "fact":
                logger.info("Appending dimension keys to data for %s", table_name)

                dim_keys = get_foreign_key(db_engine, table_name, target_schema)

                for dim in dim_keys:
                    data = append_dimension_key(db_engine, data, dim, target_schema)

                # Fill NaN in both int and float columns with 0
                data[data.select_dtypes(include=["int64", "float64"]).columns] = (
                    data.select_dtypes(include=["int64", "float64"]).fillna(0)
                )

            if check_data_exists(db_engine, target_schema, table_name, table_type):
                upsert_data(db_engine, data, table_name, target_schema)
            else:
                insert_data(db_engine, data, table_name, target_schema)
    else:
        logger.error("Table  %s does not exist in %s", table_name, target_schema)
