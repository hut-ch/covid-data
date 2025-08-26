"""
Main Transformation for Movement Indicators EU data
"""

from datetime import timedelta

import pandas as pd

from utils import (
    check_columns_exist,
    combine_data,
    create_week_start_end,
    file_check,
    get_dir,
    get_unique_data,
    is_subset,
    lookup_country_code,
    merge_rows,
    save_to_json,
)


def import_file(file: str) -> pd.DataFrame:
    """
    Import for most common format of movment indicator data from source
    json file into Pandas Dataframe then rename columns
    """

    data = pd.read_json(file)
    data.columns = data.columns.str.replace("subnational_", "regional_", regex=True)
    data.rename(
        columns={
            "CountryISO2Code": "country_code",
            "Country": "country_name",
            "country": "country_name",
            "LocationCode": "region_code",
            "geo_id_final": "region_code",
            "LocationName": "region_name",
            "region": "region_name",
            "NotificationRate": "notif_rate",
            "regional_rate_14": "regional_notif_rate",
            "NotificationRateGeoLevel": "notif_geo_level",
            "VaccineUptake": "vacc_uptake",
            "VaccineGeoLevel": "vacc_geo_level",
            "TestingRate": "test_rate",
            "TestingGeoLevel": "test_geo_level",
            "WeightedRate": "weighted_rate",
            "Colour": "colour",
            "Week": "week",
        },
        inplace=True,
    )

    return data


def create_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Create additional columns
    """
    # Columns to check
    required_columns = [
        "test_geo_level",
        "test_rate",
        "notif_geo_level",
        "notif_rate",
        "vacc_geo_level",
        "vacc_uptake",
        "regional_testing_data",
        "positivity_rate_combined",
    ]

    # Get list of columns that exist in the data
    cols_exist = check_columns_exist(data, required_columns, warn=False)[0]

    # create separate national and regional metrics
    create_national_regional(data, cols_exist)

    # Create/Transform other columns
    if "colour" in data.columns:
        data["colour"] = data["colour"].str.lower()
    if "week" in data.columns:
        create_week_start_end(data, "week")

    data = lookup_country_code(data)
    # if is_subset(data.columns, ["country_name", "country_code"]):
    #    data.loc[
    #        (data["country_name"] == "Liechtenstein") & (data["country_code"] == "NA"),
    #        "country_code",
    #    ] = "LI"
    return data


def create_national_regional(data: pd.DataFrame, cols_exist: list | None):
    """
    Create a set of national anr regional columns based on the
    level of data availble in the source

    Checks the geo-level column and adds the value to either national or regional

    This is done for the following metrics
        testing
        notifiaction
        vaccine
    """
    if cols_exist is None:
        return data

    # Columns to create
    col_map = [
        (
            ["test_geo_level", "test_rate"],
            {
                "national_test_rate": ("test_geo_level", "National", "test_rate"),
                "regional_test_rate": ("test_geo_level", "Regional", "test_rate"),
            },
        ),
        (
            ["notif_geo_level", "notif_rate"],
            {
                "national_notif_rate": ("notif_geo_level", "National", "notif_rate"),
                "regional_notif_rate": ("notif_geo_level", "Regional", "notif_rate"),
            },
        ),
        (
            ["vacc_geo_level", "vacc_uptake"],
            {
                "national_vacc_rate": ("vacc_geo_level", "National", "vacc_uptake"),
                "regional_vacc_rate": ("vacc_geo_level", "Regional", "vacc_uptake"),
            },
        ),
        (
            ["regional_testing_data", "positivity_rate_combined"],
            {
                "regional_positivity_rate": (
                    "regional_testing_data",
                    "TRUE",
                    "positivity_rate_combined",
                ),
            },
        ),
    ]

    # Dynamically create columns from mapping
    for cols_required, assignments in col_map:
        if is_subset(cols_exist, cols_required):
            for new_col, (filter_col, filter_val, source_col) in assignments.items():
                data[new_col] = data.loc[data[filter_col] == filter_val, source_col]

    return data


def cleanup_columns(source_data: pd.DataFrame, level: str) -> pd.DataFrame:
    """
    Remove columns no longer required from the dataframe ready to be output.
    Depending on the level of the data different columns are removed
    """
    common = [
        "country_code",
        "colour",
        "week",
        "week_start",
        "week_end",
    ]
    national = [
        "national_test_rate",
        "national_notif_rate",
        "national_vacc_rate",
        "national_cases_7",
        "national_cases_14",
        "national_population",
        "national_testing_rate",
        "national_positivity_rate",
    ]
    regional = [
        "region_code",
        "regional_test_rate",
        "regional_notif_rate",
        "regional_vacc_rate",
        "regional_cases_7",
        "regional_cases_14",
        "regional_population",
        "regional_testing_data",
        "regional_positivity_rate",
        "positivity_rate_combined",
        "testing_rate_combined",
    ]

    common, national, regional = (  # pylint: disable=unbalanced-tuple-unpacking
        check_columns_exist(source_data, common, national, regional)
    )

    if level == "national":
        subset = common + national
    elif level == "regional":
        subset = common + regional
    else:
        subset = common

    return source_data[subset]


def calc_national_cases_14(data: pd.DataFrame):
    """
    Create national case 14 column by looking up the national_cases_7
    value for previous week. Then add them together to get a best guess value

    First the previous week column is creatse to be matched against

    When the dataset is compared to the lookup it
        matches the current row country-code to lookup row country code
        and     the current row week-start-prev to lookup row week-start

    """

    # Create a previous week start column to use to lookup against
    data["week_start_prev"] = data["week_start"] + timedelta(weeks=-1)

    # Create a composite key to use for lookup data
    data["count_reg_week"] = (
        data["country_code"].astype(str)
        + "|"
        + data["region_code"].astype(str)
        + "|"
        + data["week_start"].astype(str)
    )

    # Create the lookup data
    prev_date_lookup = data.set_index(["count_reg_week"])["national_cases_7"]

    # Map using tuple keys, or default to 0
    data["national_cases_14"] = data.apply(
        lambda row: prev_date_lookup.get(
            (row["country_code"], row["region_code"], row["week_start_prev"]), 0
        ),
        axis=1,
    )

    data["national_cases_14"] = data["national_cases_14"] + data["national_cases_7"]

    return data


def create_datasets(
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create required datasets from transformed data"""

    # create region lookup dataset
    regions = get_unique_data(data, ["region_code", "region_name"], ["region_code"])

    # create country lookup dataset
    countries = get_unique_data(
        data, ["country_code", "country_name"], ["country_code"]
    )

    # create regional metrics dataset
    regional = cleanup_columns(data, "regional")

    # create national metrics dataset
    national = cleanup_columns(data, "national")
    national = get_unique_data(national, national.columns, ["country_code", "week"])

    return (regions, countries, regional, national)


def transform():
    """Runs transformation process for the Movement Indicators EU data"""

    print("\nTransforming EU Movement Indicators")

    file_path = get_dir("RAW_FOLDER", "eu")
    available_files = file_check(file_path, "/movementindicators*.json")
    all_data = pd.DataFrame()

    if available_files:
        print("Importing data and creating new columns")
        for file in available_files:
            data = import_file(file)
            data = create_columns(data)

            all_data = combine_data(all_data, data, combine_method="union")

        print("De-duplicating data")
        # De-duplicate data that is repeated across different files
        group_cols = ["country_code", "region_code", "week"]
        all_data = merge_rows(all_data, group_cols, aggregate=False)

        # Create missing national_cases_14 column on merged data
        mi_data = calc_national_cases_14(all_data)

        print("Creating final datasets")
        # Create final datasets from transformed data
        mi_regions, mi_countries, mi_regional, mi_national = create_datasets(mi_data)

        print("Outputting datasets")
        # save data
        datasets = [mi_regions, mi_countries, mi_regional, mi_national]
        filenames = [
            "mi-regions.json",
            "mi-country.json",
            "mi-reg-data.json",
            "mi-nat-data.json",
        ]
        save_to_json(datasets, filenames, "eu")
    else:
        print("Warning: EU Movement Indicators data not found")

    print("Completed EU Movement Indicators")
