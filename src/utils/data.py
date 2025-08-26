"""data transformation specfic utilities for the pipeline"""

import re
from typing import Any, Callable, Optional, Union

import numpy as np
import pandas as pd
from thefuzz import process

from utils.file import file_exists, get_dir, get_file


def check_columns_exist(
    data: pd.DataFrame, *col_lists: list | None, warn: Optional[bool] = None
) -> tuple:
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
            print(f"Warning: Removing columns not found in data: {missing}")

        cleaned_lists.append(existing if existing else None)

    return tuple(cleaned_lists)


def is_subset(full_list: list, sub_list: list) -> bool:
    """Checks all values in subset list exist in main list"""
    return all(val in full_list for val in sub_list)


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
    unique_data = data[col_list] if col_list else data

    test = unique_data.drop_duplicates(subset=key, keep="first")

    return test


def first_non_null(x: pd.Series):
    """Return the first non-null value, or NaN if none exist."""
    return x.dropna().iloc[0] if x.notna().any() else np.nan


AggregationFunc = Union[str, Callable[[pd.Series], Any]]


def merge_rows(
    data: pd.DataFrame, group_cols: list, aggregate: bool = True
) -> pd.DataFrame:
    """
    Merge/Deduplicate rows in a Dataframe either:
      1. Merging by picking the first non na value for a column
      2. Aggregating rows together and performing set aggregation based
      on column name conventions
    """
    # Create column to identify duplicates in data based on grouping columns provided
    data_dupes = (
        data.groupby(group_cols, dropna=False).size().reset_index(name="dupe_count")
    )

    # Split data into unique and duplicated rows returing just the key columns
    unique_keys = data_dupes.loc[data_dupes["dupe_count"] == 1, group_cols]
    duplicated_keys = data_dupes.loc[data_dupes["dupe_count"] > 1, group_cols]

    # Get full rows for each dataset
    # (this could be done in one but this make debugging easier)
    unique_data = data.merge(unique_keys, on=group_cols, how="inner")
    duplicated_data = data.merge(duplicated_keys, on=group_cols, how="inner")

    # if not duplicate return unique data
    if duplicated_data.shape[0] == 0:
        return unique_data.copy()

    if aggregate:
        # Build column-specific aggregations, this is just a
        # boilerplate example and should be updated at a later date
        aggregations: dict[str, AggregationFunc] = {}
        for col in duplicated_data.columns:
            if col in group_cols:
                continue
            if col.endswith("_set"):
                aggregations[col] = lambda x: list(set(x.dropna()))
            elif col.endswith("_sum"):
                aggregations[col] = "sum"
            elif col.endswith("_mean"):
                aggregations[col] = "mean"
            elif col.endswith("_max"):
                aggregations[col] = "max"
            elif col.endswith("_min"):
                aggregations[col] = "min"
            else:
                aggregations[col] = first_non_null

        dedupe_data = (
            duplicated_data.groupby(group_cols, dropna=False)
            .agg(aggregations)
            .reset_index()
        )
    else:
        # Apply "first non-null" row-wise in one go
        dedupe_data = (
            duplicated_data.groupby(group_cols, dropna=False).first().reset_index()
        )

    # return a complete dataset with unique and deduplicated data combined
    return combine_data(unique_data, dedupe_data, combine_method="union")


def combine_data(*data: pd.DataFrame, combine_method: str = "union"):
    """
    Combines any number of dataframes into a single Dataframe

    Args:
    *data: One or more pandas DataFrames.
    combine_method: Combination type:
        'union' -> vertical stack (row union)
        'inner' -> horizontal join on common index
        'left'  -> horizontal join aligned to first DataFrame's index
    """

    if combine_method == "union":
        combined = pd.concat(data, axis=0, ignore_index=True)
    elif combine_method == "inner":
        combined = pd.concat(data, axis=1, join=combine_method)
    elif combine_method == "left":
        combined = pd.concat(data, axis=1).reindex(data[0].index)
    else:
        raise ValueError(f"Unknown combination type: {type}")

    return combined


def create_week_start_end(data: pd.DataFrame, week_col: str) -> pd.DataFrame:
    """
    Create a week-start and week-end date columns
    based on input week column (YYYY-ww) or (YYYY-Www)
    """
    if week_col in data.columns:
        split_cols = data[week_col].str.split("-", n=1, expand=True)
        data["year"] = split_cols[0].astype(int)
        data["week"] = split_cols[1].str[-2:].astype(int)

        # Use ISO calendar to calculate week start and end
        data["week_start"] = data.apply(
            lambda r: pd.to_datetime(
                f"{r['year']}-W{int(r['week']):02d}-1", format="%G-W%V-%u"
            ),
            axis=1,
        )
        data["week_end"] = data["week_start"] + pd.Timedelta(days=6)

    return data


def camel_to_snake(name: str) -> str:
    """Convert a 'CamelCase' or 'camelCase' string to 'camel_case'"""

    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)  # Handle first capital in word
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)  # Handle adjacent capitals

    return name.lower()


def perform_fuzzy_match(data: pd.DataFrame, mask: pd.Series, lookup: pd.DataFrame):
    """run a fizzy match over country names data using the iso country lookup"""

    # Get distinct list of unmatched countries
    country_list = (
        data[["country_name", "country_code_lk"]]
        .drop_duplicates()
        .loc[mask, "country_name"]
    )

    # run fuzzy match and return the first result
    # along with the original country
    matches = [
        [
            process.extract(country.lower(), lookup["Name"], limit=1)[0][:2],
            country,
        ]
        for country in country_list
    ]

    # Convert matches to DataFrame
    match_df = pd.DataFrame(
        # Unpack the nested list structure
        [(match[0][0], match[0][1], match[1]) for match in matches],
        columns=["country_match", "country_score", "country_name"],
    )

    # Remove any that match lower than 75
    match_df = match_df.loc[match_df["country_score"] >= 75]

    # search the mapping again based on the result of the fuzzy match
    match_df["country_code_fuzzy"] = (
        match_df["country_match"].str.lower().map(lookup.set_index("Name")["Code"])
    )

    # Update only the rows for countries that needed fuzzy matching
    data = data.merge(
        match_df[
            ["country_name", "country_match", "country_score", "country_code_fuzzy"]
        ],
        on="country_name",
        how="left",
    )

    data["country_code_lk"] = data["country_code_lk"].fillna(data["country_code_fuzzy"])

    return data


def lookup_country_code(data: pd.DataFrame) -> pd.DataFrame:
    """check the country name and return the 2 letter country code"""

    file_path = get_dir("RAW_FOLDER", "lookup")

    if file_exists(file_path, "data.csv"):
        file = get_file(file_path, "data.csv")
        country_lkup = pd.read_csv(file)
        country_lkup["Name"] = country_lkup["Name"].str.lower()

        # Firstly check against lookup table to get code
        data["country_code_lk"] = (
            data["country_name"].str.lower().map(country_lkup.set_index("Name")["Code"])
        )

        mask = data["country_code_lk"].isnull()

        if mask.any():
            # fix a special case as it will always match to US
            data.loc[
                data["country_name"] == "United States Virgin Islands", "country_name"
            ] = "Virgin Islands US"

            # run fuzzy mach on country name
            data = perform_fuzzy_match(data, mask, country_lkup)
            # revert fix
            data.loc[data["country_name"] == "Virgin Islands US", "country_name"] = (
                "United States Virgin Islands"
            )
            data = data.drop(
                columns=["country_code_fuzzy", "country_match", "country_score"]
            )

        if "territory_code" in data.columns:
            # For any nulls, try using first 2 chars of territory_code
            mask = data["country_code_lk"].isnull() & data["territory_code"].notna()
            data.loc[mask, "country_code_lk"] = data.loc[mask, "territory_code"].str[:2]

        if "country_code" in data.columns:
            # Finally if still null use original value in country code
            mask = data["country_code_lk"].isnull() & data["country_code"].notna()
            data.loc[mask, "country_code_lk"] = data.loc[mask, "country_code"]

        data["country_code"] = data["country_code_lk"]

        data = data.drop(columns=["country_code_lk"])

    return data
