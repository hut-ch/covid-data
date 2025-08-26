"""Main transformation for National Deaths EU data"""

import pandas as pd

from utils import (
    check_columns_exist,
    combine_data,
    create_week_start_end,
    file_check,
    get_dir,
    get_unique_data,
    is_subset,
    load_json,
    lookup_country_code,
    merge_rows,
    save_to_json,
)


def import_file(file: str) -> pd.DataFrame:
    """
    Import for most common format of movment indicator data from source
    json file into Pandas Dataframe then rename columns
    """

    data = load_json(file)
    data.rename(
        columns={
            "country_code": "territory_code",
            "geoId": "country_code",
            "countriesAndTerritories": "country_name",
            "country": "country_name",
            "countryterritoryCode": "territory_code",
            "continentExp": "continent",
            "popData2020": "population",
            "source": "source_name",
        },
        inplace=True,
    )

    return data


def set_data_types(data: pd.DataFrame):
    """
    Dynamically set data types for columns
    """

    col_list = [
        "cases",
        "cases_cumulative",
        "cases_notif_rate_14",
        "deaths",
        "deaths_cumulative",
        "deaths_notif_rate_14",
        "population",
    ]
    col_dtypes = ["Int64", "Int64", "Float64", "Int64", "Int64", "Float64", "Int64"]

    col_conversion = dict(zip(col_list, col_dtypes))

    cols = check_columns_exist(data, col_list, warn=False)[0]

    for col in cols:
        data[col] = data[col].astype(col_conversion[col])

    return data


def assign_level(data: pd.DataFrame) -> pd.DataFrame | None:
    """check if the data loaded is weekly or daily"""
    required_cols = ["year_week", "day"]
    actual_cols = check_columns_exist(data, required_cols, warn=False)[0]

    if is_subset(actual_cols, ["day"]):
        data["level"] = "daily"
    elif is_subset(actual_cols, ["year_week"]):
        data["level"] = "weekly"
    else:
        print("Cannot determine level of data")
        return None

    return data


def create_cases_deaths(data: pd.DataFrame, cols: list | None):
    """
    Create a set of cases and deaths columns based on the
    level of data available in the source

    Checks the indicator column and adds the value to either cases or deaths

    This is done for the following metrics
        weekly_count
        cumulative_count
        rate_14_day
    """

    if cols is None:
        return data

    if "year_week" in cols:
        # Columns to create
        col_map = [
            (
                ["indicator", "weekly_count"],
                {
                    "cases": ("indicator", "cases", "weekly_count"),
                    "deaths": ("indicator", "deaths", "weekly_count"),
                },
            ),
            (
                ["indicator", "cumulative_count"],
                {
                    "cases_cumulative": ("indicator", "cases", "cumulative_count"),
                    "deaths_cumulative": ("indicator", "deaths", "cumulative_count"),
                },
            ),
            (
                ["indicator", "rate_14_day"],
                {
                    "cases_notif_rate_14": ("indicator", "cases", "rate_14_day"),
                    "deaths_notif_rate_14": ("indicator", "deaths", "rate_14_day"),
                },
            ),
        ]

        # Dynamically create columns from mapping
        for cols_required, assignments in col_map:
            if is_subset(cols, cols_required):
                for new_col, (
                    filter_col,
                    filter_val,
                    source_col,
                ) in assignments.items():
                    data[new_col] = data.loc[data[filter_col] == filter_val, source_col]

    elif "date" in cols:
        data["cases_cumulative"] = (
            data.sort_values(["date"]).groupby(["territory_code"])["cases"].cumsum()
        )
        data["deaths_cumulative"] = (
            data.sort_values(["date"]).groupby(["territory_code"])["deaths"].cumsum()
        )
    return data


def get_rate_from_weekly(daily: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    """
    Retireve 14 day notification rates from the weekly data
    rather than recalculate based on the daily data
    """
    daily.drop(
        ["cases_notif_rate_14", "deaths_notif_rate_14"], axis="columns", inplace=True
    )

    merged = daily.merge(
        weekly[
            [
                "territory_code",
                "week_start",
                "week_end",
                "cases_notif_rate_14",
                "deaths_notif_rate_14",
            ]
        ],
        on="territory_code",
        how="left",
    )

    merged = merged[
        (merged["date"] >= merged["week_start"])
        & (merged["date"] <= merged["week_end"])
    ]

    merged = get_unique_data(merged, merged.columns, ["territory_code", "date"])

    return merged


def create_columns(data: pd.DataFrame) -> pd.DataFrame:
    """create required columns"""

    if "year_week" in data.columns:
        create_week_start_end(data, "year_week")
    if is_subset(data, ["day", "month", "year"]):
        data["date"] = pd.to_datetime(data[["year", "month", "day"]])
    if "source_name" not in data.columns:
        data["source_name"] = (
            "Ministries of Health or National Public Health Institutes"
        )

    data["territory_code"] = data["territory_code"].fillna("continent")

    data = lookup_country_code(data)

    required_cols = [
        "year_week",
        "indicator",
        "weekly_count",
        "cumulative_count",
        "rate_14_day",
        "date",
        "cases",
        "deaths",
    ]
    actual_cols = check_columns_exist(data, required_cols, warn=False)[0]
    create_cases_deaths(data, actual_cols)

    return data


def cleanup_columns(source_data: pd.DataFrame, level: str) -> pd.DataFrame:
    """
    Remove columns no longer required from the dataframe ready to be output.
    Depending on the level of the data different columns are removed
    """
    common = [
        "territory_code",
        "continent",
        "population",
        "cases",
        "cases_cumulative",
        "cases_notif_rate_14",
        "deaths",
        "deaths_cumulative",
        "deaths_notif_rate_14",
        "source_name",
        "country_name",
        "country_match",
        "country_score",
    ]
    weekly = ["year_week", "week_start", "week_end"]
    daily = ["date"]

    common, weekly, daily = (  # pylint: disable=unbalanced-tuple-unpacking
        check_columns_exist(source_data, common, weekly, daily)
    )

    if level == "weekly":
        subset = weekly + common
    elif level == "daily":
        subset = daily + common
    else:
        subset = common

    return source_data[subset]


def create_datasets(
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create required datasets from transformed data"""

    # create country lookup dataset
    cols = ["country_code", "country_name", "territory_code", "continent"]
    countries = data[data["territory_code"] != "continent"].copy()
    countries = get_unique_data(countries, cols, ["territory_code"])

    # create country lookup dataset
    cols = ["continent"]
    continents = get_unique_data(data, cols, ["continent"])

    # create country lookup dataset
    cols = ["source_name"]
    sources = get_unique_data(data, cols, ["source_name"])

    # create daily metrics dataset
    daily = data[data["level"] == "daily"].copy()
    daily = cleanup_columns(daily, "daily")

    # create weekly metrics dataset
    weekly = data[data["level"] == "weekly"].copy()
    weekly = cleanup_columns(weekly, "weekly")

    merge_key = ["year_week", "territory_code"]
    weekly = merge_rows(weekly, merge_key, aggregate=False)

    return (countries, continents, sources, daily, weekly)


def transform():
    """Runs transformation process for the National Case Death EU data"""

    print("\nTransforming EU National Case Deaths")

    file_path = get_dir("RAW_FOLDER", "eu")
    available_files = file_check(file_path, "/nationalcasedeath*.json")
    full_data = pd.DataFrame()

    if available_files:
        print("Importing data and creating new columns")
        for file in available_files:
            data = import_file(file)
            data = assign_level(data)
            if data is None:
                continue
            data = create_columns(data)

            full_data = combine_data(full_data, data, combine_method="union")

        print("Creating final datasets")
        country_lkup, cont_lkup, source_lkup, daily_data, weekly_data = create_datasets(
            full_data
        )

        print("Getting notification rates for daily")
        daily_data = get_rate_from_weekly(daily_data, weekly_data)

        print("Setting Datatypes")
        daily_data = set_data_types(daily_data)
        weekly_data = set_data_types(weekly_data)

        print("Outputting datasets")
        # save data
        datasets = [country_lkup, cont_lkup, source_lkup, daily_data, weekly_data]
        filenames = [
            "nd-country.json",
            "nd-continents.json",
            "nd-sources.json",
            "nd-daily-nat-data.json",
            "nd-weekly-nat-data.json",
        ]
        save_to_json(datasets, filenames, "eu")
    else:
        print("Warning: EU Movement Indicators data not found")

    print("Completed EU National Case Deaths")
