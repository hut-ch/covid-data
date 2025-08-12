"""Main Transformation for Movement Indicators EU data"""

import pandas as pd
from pipeline.utils import (check_columns_exist, combine_data, create_dir,
                            file_check, get_dir, get_file, get_unique_data)


def import_standard(file_path: str, file: str) -> pd.DataFrame:
    """
    Import for most common format of movment indicator data
    from source json file into Pandas Dataframe
    then rename columns

    Args:
    file_path: the path to the josn file to be loaded.
    file: the name of the json file to be loaded

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    file = get_file(file_path, file)
    data = pd.read_json(file)
    data.columns = [
        "country_code",
        "country",
        "region_code",
        "region",
        "notif_rate",
        "notif_geo_level",
        "vacc_uptake",
        "vacc_geo_level",
        "test_rate",
        "test_geo_level",
        "weighted_rate",
        "colour",
        "week",
    ]
    return data


def import_other(file_path: str, file: str) -> pd.DataFrame:
    """
    Import for other format of movment indicator data
    from source json file into Pandas Dataframe
    then rename columns

    Args:
    file_path: the path to the josn file to be loaded.
    file: the name of the json file to be loaded

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    file = get_file(file_path, file)
    data = pd.read_json(file)
    data.columns = data.columns.str.replace("subnational_", "regional_", regex=True)

    data.rename(
        columns={
            "geo_id_final": "region_code",
            "regional_rate_14": "regional_notif_rate",
        },
        inplace=True,
    )

    return data


def create_columns_standard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create additional required columns for standard data.
    Validates tha required columns exist in source data
    before creating new columns

    Args:
    df: Source Pandas DataFrame to create columns.

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    col_check = check_columns_exist(
        df,
        [
            "test_geo_level",
            "test_rate",
            "notif_geo_level",
            "notif_rate",
            "vacc_geo_level",
            "vacc_uptake",
        ],
    )

    if col_check:
        data = df.assign(
            source="standard",
            national_test_rate=df.loc[df["test_geo_level"] == "National", "test_rate"],
            regional_test_rate=df.loc[df["test_geo_level"] == "Regional", "test_rate"],
            national_notif_rate=df.loc[
                df["notif_geo_level"] == "National", "notif_rate"
            ],
            regional_notif_rate=df.loc[
                df["notif_geo_level"] == "Regional", "notif_rate"
            ],
            national_vacc_rate=df.loc[
                df["vacc_geo_level"] == "National", "vacc_uptake"
            ],
            regional_vacc_rate=df.loc[
                df["vacc_geo_level"] == "Regional", "vacc_uptake"
            ],
        )

        data["colour"].str.lower()
    else:
        data = df
    return data


def create_columns_other(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create additional required columns for other data.
    Validates tha required columns exist in source data
    before creating new columns

    Args:
    df: Source Pandas DataFrame to create columns.

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    col_check = check_columns_exist(
        df, ["regional_testing_data", "positivity_rate_combined"]
    )
    if col_check:
        data = df.assign(
            source="other",
            regional_positivity_rate=df.loc[
                df["regional_testing_data"] == "TRUE", "positivity_rate_combined"
            ],
        )

        data["colour"].str.lower()
    else:
        data = df
    return data


def cleanup_columns(source_data: pd.DataFrame, level: str) -> pd.DataFrame:
    """
    Remove columns no longer required form the dataframe ready to be output.
    Depending on the level of the data different columns are removed

    Args:
    source_data: Source Pandas DataFrame to create columns.
    level: either national or regional to remove columns
    related to other level.

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    common = [
        "country",
        "region",
        "notif_rate",
        "notif_geo_level",
        "vacc_uptake",
        "vacc_geo_level",
        "test_rate",
        "test_geo_level",
    ]
    national = [
        "regional_test_rate",
        "regional_notif_rate",
        "regional_vacc_rate",
        "regional_cases_7",
        "regional_cases_14",
        "regional_population",
    ]
    regional = [
        "national_test_rate",
        "national_notif_rate",
        "national_vacc_rate",
        "national_cases_7",
        "national_cases_14",
        "national_population",
    ]
    common, national, regional = (  # pylint: disable=unbalanced-tuple-unpacking
        check_columns_exist(source_data, common, national, regional)
    )

    data = source_data.drop(common, axis="columns")

    if level == "national":
        data = data.drop(national, axis="columns")
    elif level == "regional":
        data = data.drop(regional, axis="columns")

    return data


def output_data(datasets, file_names):
    """
    Outputs the given Dataframes as json files into the
    cleansed-data folder with the given filename

    Args:
    datasets: Source Pandas DataFrame to create columns.
    file_names: either national or regional to remove columns
    related to other level.

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    save_dir = get_dir("cleansed-data", "eu")
    create_dir(save_dir)

    for dataset, filename in zip(datasets, file_names):
        file = get_file(save_dir, filename)
        dataset.reset_index(drop=True, inplace=True)
        dataset.to_json(file)


def transform():
    """Peforms transformation process the Movement Indicators EU data"""

    file_list = [
        "movementindicators.json",
        "movementindicatorsarchive.json",
        "movementindicatorsarchive2022.json",
    ]
    file_path = get_dir("raw-folder", "eu")
    available_files = file_check(file_list, file_path)
    mi_data = pd.DataFrame()

    if available_files is not None:
        for file in available_files:
            if file != "movementindicatorsarchive.json":
                data = import_standard(file_path, file)
                data = create_columns_standard(data)
            else:
                data = import_other(file_path, file)
                data = create_columns_other(data)

            mi_data = combine_data(mi_data, data)

        mi_countries = get_unique_data(
            mi_data, ["country_code", "country"], ["country_code"]
        )
        mi_regions = get_unique_data(
            mi_data, ["region_code", "region"], ["region_code"]
        )
        mi_national = cleanup_columns(mi_data, "national")
        mi_regional = cleanup_columns(mi_data, "regional")

        datasets = [mi_regions, mi_countries, mi_national, mi_regional]
        filenames = [
            "mi-regions.json",
            "mi-countries.json",
            "mi-national-data.json",
            "mi-regional-data.json",
        ]

        output_data(datasets, filenames)
    else:
        print("No movement indicators data found to skipping transform")

    print("Transformed EU Movement Indicators")
