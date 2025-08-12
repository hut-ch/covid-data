"""data transformation specfic utilities for the pipeline"""

import pandas as pd


def check_columns_exist(data: pd.DataFrame, *col_lists: list | None, warn: bool = True):
    """
    Given one or more column lists, return versions with only the columns
    that exist in the dataframe. Missing columns are removed
    (with optional warning).

    Args:
        df: DataFrame to check against.
        *col_lists: Any number of lists (or None).
        warn: If True, print a warning when columns are missing.

    Returns:
        A tuple of cleaned column lists (in same order as given).
    """
    cleaned_lists: list[list | None] = []
    for col_list in col_lists:
        if col_list is None:
            cleaned_lists.append(None)
            continue

        existing = [c for c in col_list if c in data.columns]
        missing = set(col_list) - set(data.columns)

        if missing and warn:
            print(f"Warning: Droping columns not found in data: {missing}")

        cleaned_lists.append(existing if existing else None)

    return tuple(cleaned_lists)


def get_unique_data(data: pd.DataFrame, col_list: list | None, key: list | None):
    """
    Takes the input DataFrame and removes and duplicated rows
    based on arguments provided.

    If no argurement are provided checks the whole dataframe
    or a subset of columns and oly retuns those columns.
    Optionally uses the keys list to determine which rows are
    duplicated.

    Args:
        data: source DataFrame to get unqiue data from.
        column_list: optionaL list of columns to check and return.
        key: optional list of columns to use for uniqueness.

    Returns:
        A Pandas Dataframe of unique data.
    """
    col_list, key = check_columns_exist(  # pylint: disable=unbalanced-tuple-unpacking
        data, col_list, key
    )

    df = data[col_list] if col_list else data
    return df.drop_duplicates(subset=key, keep="first")


def combine_data(*data: pd.DataFrame):
    """
    Combines any number of datafeames into a seimngle Dataframe

    Args:
        *data: any number of Pandas Dataframes to be combined.

    Returns:
        A Pandas Dataframe of data.
    """
    return pd.concat([*data])
