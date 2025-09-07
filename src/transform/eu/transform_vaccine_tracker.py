"""Main transformation for Vaccine Tracker EU data"""

import hashlib

import numpy as np
import pandas as pd

from utils import (
    camel_to_snake,
    check_columns_exist,
    create_week_start_end,
    file_check,
    get_dir,
    get_logger,
    get_unique_data,
    load_json,
    load_json_chunk,
    save_chunk_to_json,
    save_to_json,
)

logger = get_logger(__name__)
# import timeit

# from line_profiler import LineProfiler
# from memory_profiler import profile


def rename_cols(data: pd.DataFrame) -> pd.DataFrame:
    """
    Import for most common format of movment indicator data from source
    json file into Pandas Dataframe then rename columns
    """
    data.rename(
        columns={
            "YearWeekISO": "year_week",
            "ReportingCountry": "country_code",
            "Region": "region_code",
            "TargetGroup": "target_group",
            "Vaccine": "vaccine_code",
        },
        inplace=True,
    )
    data.columns = [camel_to_snake(col) for col in data.columns]

    data.columns = data.columns.str.replace(
        "dose_additional", "additional_dose_", regex=True
    )
    data.columns = data.columns.str.replace("dose_unknown", "unknown_dose", regex=True)
    data.columns = data.columns.str.replace("number_doses", "doses", regex=True)

    data = set_types(data)

    return data


def create_col_key(row):
    """Generate a hash from multiple key columns"""
    key = f"{row['year_week']}|{row['country_code']}|{row['region_code']}|\
        {row['target_group']}|{row['vaccine_code']}"
    return hashlib.sha256(key.encode()).hexdigest()


def set_types(data: pd.DataFrame) -> pd.DataFrame:
    """set any columns types if not auto detected"""

    data["doses_received"] = pd.to_numeric(data["doses_received"], errors="coerce")
    data["doses_exported"] = pd.to_numeric(data["doses_exported"], errors="coerce")
    data["denominator"] = pd.to_numeric(data["denominator"], errors="coerce")

    # Fill NaN with 0 and convert to int
    data = data.fillna(0).astype(
        {
            "doses_received": "int64",
            "doses_exported": "int64",
            "denominator": "int64",
        }
    )

    return data


def set_level(data: pd.DataFrame):
    """determine if the row applies to a country or region"""
    logger.info("Determining level of data (National or Regional)")

    required_cols = ["region_code", "country_code"]
    cols = check_columns_exist(data, required_cols)
    if cols:
        data["level"] = np.where(
            data["region_code"] == data["country_code"], "country", "region"
        )

    return data


def get_age_boundries(group: str) -> tuple[int | None, int | None]:
    """Define upper and lower limits of age banding bases on age grouping column"""
    if group.startswith("Age") or group.startswith("1_Age"):

        # trim prefix
        if group.startswith("Age"):
            age_part = group[3:]
        else:
            age_part = group[5:]

        if "_" in age_part:  # Format: 10_14
            lower, upper = age_part.split("_")
            return int(lower), int(upper)
        if "<" in age_part:  # Format: <18
            upper = age_part.replace("<", "")
            return 0, int(upper)
        if "+" in age_part:  # Format: 80+
            lower = age_part.replace("+", "")
            return int(lower), 200
    return 0, 200  # "ALL, AGEUnk, H, LTCF"


def create_age_range(data: pd.DataFrame):
    """
    Assign upper and lower age ranges based on age grouping column
    using assigned function
    """
    logger.info("Creating age boundaries")
    cols = ["target_group"]
    target_desc = {
        "ALL": "Overall adults (18+)",
        "Age<18": "Overall adolescents and children (0-17 years old)",
        "HCW": "Healthcare workers",
        "LTCF": "Residents in long term care facilities",
        "Age0_4": "0-4 years old",
        "Age5_9": "5-9 years old",
        "Age10_14": "10-14 years old",
        "Age15_17": "15-17 years old",
        "Age18_24": "18-24 years old",
        "Age25_49": "25-49 years old",
        "Age50_59": "50-59 years old",
        "Age60_69": "60-69 years old",
        "Age70_79": "70-79 years old",
        "Age80+": "80 years and over",
    }

    valid_cols = check_columns_exist(data, cols)
    if valid_cols:
        data[["age_lower_limit", "age_upper_limit"]] = data["target_group"].apply(
            lambda x: pd.Series(get_age_boundries(x))
        )

        data["target_description"] = (
            data["target_group"].map(target_desc).fillna(data["target_group"])
        )

    return data


def create_datasets(
    data: pd.DataFrame,
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """Create required datasets from transformed data"""

    # create country
    logger.info("Creating country lookup dataset")
    cols = ["country_code"]
    countries = get_unique_data(data, cols, ["country_code"])

    # create vaccine
    logger.info("Creating vaccine lookup dataset")
    cols = ["vaccine_code"]
    vaccines = get_unique_data(data, cols, ["vaccine_code"])

    # create age
    logger.info("Creating target lookup dataset")
    cols = ["target_group", "age_lower_limit", "age_upper_limit", "target_description"]
    ages = get_unique_data(data, cols, ["target_group"])

    # create national metrics
    logger.info("Creating national metrics dataset")
    national = data[data["level"] == "country"].copy()
    national = national.drop(
        columns=[
            "year",
            "week",
            "year_week",
            "level",
            "region_code",
            "age_lower_limit",
            "age_upper_limit",
        ]
    )

    # create regional metrics
    logger.info("Creating regional metrics dataset")
    regional = data[data["level"] == "region"].copy()
    regional = regional.drop(
        columns=[
            "year",
            "week",
            "year_week",
            "level",
            "age_lower_limit",
            "age_upper_limit",
        ]
    )

    # create region
    logger.info("Creating region lookup dataset")
    cols = ["region_code"]
    regions = get_unique_data(regional, cols, ["region_code"])

    return (countries, regions, vaccines, ages, national, regional)


# @profile
def transform_chunk(env_vars: dict | None):
    """Runs transformation process for the National Case Death EU data"""

    logger.info("Transforming EU Vaccine Tracker - in chunks")

    available_files = file_check(
        get_dir("RAW_FOLDER", "eu", env_vars), "/vaccine-tracker*.json"
    )

    if available_files:
        for file in available_files:
            logger.info("Processing %s", file)
            # processed_keys = set()
            for i, chunk in enumerate(load_json_chunk(file, 50000)):
                logger.info("Processing %s Chunk %s", file, i)

                chunk = rename_cols(chunk)
                chunk = set_level(chunk)
                chunk = create_age_range(chunk)
                chunk = create_week_start_end(chunk, "year_week")

                logger.info("Creating Final Datasets for Chunk %s", i)

                (
                    countries_lookup,
                    regions_lookup,
                    vaccines_lookup,
                    target_group_lookup,
                    national_data,
                    regional_data,
                ) = create_datasets(chunk)

                # output datasets
                datasets = [
                    countries_lookup,
                    regions_lookup,
                    vaccines_lookup,
                    target_group_lookup,
                    national_data,
                    regional_data,
                ]
                filenames = [
                    "vt-dim_country.json",
                    "vt-dim_region.json",
                    "vt-dim_vaccine.json",
                    "vt-dim_target_group.json",
                    "vt-fact_vaccine_tracker_country.json",
                    "vt-fact_vaccine_tracker_region.json",
                ]

                for dataset, filename in zip(datasets, filenames):
                    save_chunk_to_json(dataset, filename, "eu", env_vars, i == 0)


# @profile
def transform_whole(env_vars: dict | None):
    """Runs transformation process for the National Case Death EU data"""

    logger.info("Transforming EU Vaccine Tracker")

    file_path = get_dir("RAW_FOLDER", "eu", env_vars)
    available_files = file_check(file_path, "/vaccine-tracker*.json")

    if available_files:
        for file in available_files:
            logger.info("Processing %s", file)

            data = load_json(file)

            data = rename_cols(data)
            data = set_level(data)
            data = create_age_range(data)
            data = create_week_start_end(data, "year_week")

            logger.info("Creating Final Datasets")

            (
                countries_lookup,
                regions_lookup,
                vaccines_lookup,
                ages_lookup,
                national_data,
                regional_data,
            ) = create_datasets(data)

            # output datasets
            datasets = [
                countries_lookup,
                regions_lookup,
                vaccines_lookup,
                ages_lookup,
                national_data,
                regional_data,
            ]
            filenames = [
                "vtw-dim_country.json",
                "vtw-dim_region.json",
                "vtw-dim_vaccine.json",
                "vtw-dim_age.json",
                "vtw-fact_vaccine_tracker_country.json",
                "vtw-fact_vaccine_tracker_region.json",
            ]

            save_to_json(datasets, filenames, "eu", env_vars)


def transform(env_vars: dict | None):
    """run timing to defermine if full or chunk load/prosessing is best"""

    transform_chunk(env_vars)
    # whole_time = timeit.timeit("transform_whole()", globals=globals(), number=3)

    # transform_whole(env_vars)

    # chunked_time = timeit.timeit("transform_chunk()", globals=globals(), number=3)
    # logger.info(datetime.datetime.now())

    # logger.info("Whole file avg time: %ss", whole_time/3:.3f)
    # logger.info("Chunked time: %ss", chunked_time/3:.3f)

    # logger.info("profiling")
    # lp = LineProfiler()
    # lp_wrapper = lp(transform_whole())
    # lp_wrapper()
    # lp.print_stats()

    # lp_wrapper = lp(transform_chunk())
    # lp_wrapper()
    # lp.print_stats()
    # lp.print_stats()
