"""Main Transformation for Movement Indicators EU data"""

from datetime import timedelta

import pandas as pd

from utils import (
    check_columns_exist,
    combine_data,
    create_week_start_end,
    file_check,
    get_dir,
    get_logger,
    get_unique_data,
    is_subset,
    lookup_country_code,
    merge_rows,
    save_to_json,
)

logger = get_logger(__name__)


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
            "Colour": "colour_name",
            "colour": "colour_name",
            "Week": "week",
        },
        inplace=True,
    )

    return data


def create_columns(data: pd.DataFrame, env_vars: dict | None) -> pd.DataFrame:
    """Create additional columns"""

    logger.info("Creating required missing columns")

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
    if "colour_name" in data.columns:
        data["colour_name"] = data["colour_name"].str.lower()

        colour_desc = {
            "green": "Weighted rate is less than 40",
            "orange": "Weighted rate is less than 100 but 40 or more",
            "red": "Weighted rate is less than 300 but 100 or more",
            "dark red": "Weighted rate is 300 or more",
            "dark grey": "Testing rate is 600 or less",
            "grey": "Insufficient data is available",
            "grey - no data": "no data",
        }
        data["colour_description"] = (
            data["colour_name"].map(colour_desc).fillna(data["colour_name"])
        )

    if "week" in data.columns:
        create_week_start_end(data, "week")

    data = lookup_country_code(data, env_vars)

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

    logger.info("Creating missing national and regional columns")

    if cols_exist is None:
        return data

    # Columns to create
    col_map = [
        (
            ["test_geo_level", "test_rate"],
            {
                "national_testing_rate": ("test_geo_level", "National", "test_rate"),
                "regional_testing_rate": ("test_geo_level", "Regional", "test_rate"),
            },
        ),
        (
            ["notif_geo_level", "notif_rate"],
            {
                "national_notification_rate": (
                    "notif_geo_level",
                    "National",
                    "notif_rate",
                ),
                "regional_notification_rate": (
                    "notif_geo_level",
                    "Regional",
                    "notif_rate",
                ),
            },
        ),
        (
            ["vacc_geo_level", "vacc_uptake"],
            {
                "national_vaccination_rate": (
                    "vacc_geo_level",
                    "National",
                    "vacc_uptake",
                ),
                "regional_vaccination_rate": (
                    "vacc_geo_level",
                    "Regional",
                    "vacc_uptake",
                ),
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

    logger.info("Removing columns no longer required")

    common = [
        "country_code",
        "colour_name",
        "week",
        "week_start_date",
        "week_end_date",
    ]
    national = [
        "national_notification_rate",
        "national_vaccination_rate",
        "national_cases_7",
        "national_cases_14",
        "national_population",
        "national_testing_rate",
        "national_positivity_rate",
    ]
    regional = [
        "region_code",
        "regional_notification_rate",
        "regional_vaccination_rate",
        "regional_cases_7",
        "regional_cases_14",
        "regional_population",
        "regional_testing_rate",
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

    logger.info("Calculating 14 day rate")

    # Create a previous week start column to use to lookup against
    data["week_start_date_prev"] = data["week_start_date"] + timedelta(weeks=-1)

    # Create a composite key to use for lookup data
    data["count_reg_week"] = (
        data["country_code"].astype(str)
        + "|"
        + data["region_code"].astype(str)
        + "|"
        + data["week_start_date"].astype(str)
    )

    # Create the lookup data
    prev_date_lookup = data.set_index(["count_reg_week"])["national_cases_7"]

    # Map using tuple keys, or default to 0
    data["national_cases_14"] = data.apply(
        lambda row: prev_date_lookup.get(
            (row["country_code"], row["region_code"], row["week_start_date_prev"]), 0
        ),
        axis=1,
    )

    data["national_cases_14"] = data["national_cases_14"] + data["national_cases_7"]

    return data


def create_datasets(
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create required datasets from transformed data"""

    # create region
    logger.info("Creating region lookup dataset")
    regions = get_unique_data(data, ["region_code", "region_name"], ["region_code"])

    # create country
    logger.info("Creating country lookup dataset")
    countries = get_unique_data(
        data, ["country_code", "country_name"], ["country_code"]
    )

    # create country
    logger.info("Creating status lookup dataset")
    status = get_unique_data(
        data, ["colour_name", "colour_description"], ["colour_name"]
    )

    # create regional metrics
    logger.info("Creating regional metrics dataset")
    regional = cleanup_columns(data, "regional").copy()
    regional.columns = regional.columns.str.replace("regional_", "")
    regional.rename(
        columns={
            "cases_7": "new_cases_last_7days",
            "cases_14": "new_cases_last_14days",
        },
        inplace=True,
    )

    # create national metrics
    logger.info("Creating national metrics dataset")
    national = cleanup_columns(data, "national").copy()
    national.columns = national.columns.str.replace("national_", "")
    national.rename(
        columns={
            "cases_7": "new_cases_last_7days",
            "cases_14": "new_cases_last_14days",
        },
        inplace=True,
    )
    national = get_unique_data(national, national.columns, ["country_code", "week"])

    return (regions, countries, status, regional, national)


def transform(env_vars: dict | None):
    """Runs transformation process for the Movement Indicators EU data"""

    logger.info("Transforming EU Movement Indicators")

    file_path = get_dir("RAW_FOLDER", "eu", env_vars)
    available_files = file_check(file_path, "/movementindicators*.json")
    all_data = pd.DataFrame()

    if available_files:
        for file in available_files:
            logger.info("Processing %s", file)

            data = import_file(file)
            data = create_columns(data, env_vars)

            all_data = combine_data(all_data, data, combine_method="union")

        # De-duplicate data that is repeated across different files
        group_cols = ["country_code", "region_code", "week"]
        all_data = merge_rows(all_data, group_cols, aggregate=False)

        # Create missing national_cases_14 column on merged data
        mi_data = calc_national_cases_14(all_data)

        logger.info("Creating final datasets")
        # Create final datasets from transformed data
        mi_regions, mi_countries, mi_status, mi_regional, mi_national = create_datasets(
            mi_data
        )

        # save data
        datasets = [mi_regions, mi_countries, mi_status, mi_regional, mi_national]
        filenames = [
            "mi-dim_region.json",
            "mi-dim_country.json",
            "mi-dim_status.json",
            "mi-fact_movement_indicators_region.json",
            "mi-fact_movement_indicators_country.json",
        ]

        save_to_json(datasets, filenames, "eu", env_vars)
    else:
        logger.warning("No EU Movement Indicators data found")

    logger.info("Completed EU Movement Indicators")
