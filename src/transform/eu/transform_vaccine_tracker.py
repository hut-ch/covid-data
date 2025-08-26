"""Main transformation for Vaccine Tracker EU data"""

import datetime

import ijson
import numpy as np
import pandas as pd

from utils import (
    camel_to_snake,
    check_columns_exist,
    create_week_start_end,
    file_check,
    get_dir,
    get_unique_data,
    load_json,
    save_chunk_to_json,
    save_to_json,
)

# import timeit

# from line_profiler import LineProfiler
# from memory_profiler import profile


def load_json_chunk(file: str, chunk_size: int = 1000):
    """
    Stream and load JSON records from a large JSON file in chunks.
    The JSON must be a dict with a 'records' key, whose value is a large array.

    Yields:
        pd.DataFrame: DataFrame chunk of specified size
    """
    with open(file, "r", encoding="utf-8") as f:
        objects = ijson.items(f, "records.item")
        buffer = []

        for obj in objects:
            buffer.append(obj)
            if len(buffer) >= chunk_size:
                yield pd.DataFrame(buffer)
                buffer = []

        if buffer:
            yield pd.DataFrame(buffer)


def finalize_json_file(path):
    """
    Finalizes a streamed JSON array file by closing it with a ']'.
    """
    with open(path, "a", encoding="utf-8") as f:
        f.write("]")


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
            "TargetGroup": "age_group",
            "Vaccine": "vaccine_code",
        },
        inplace=True,
    )
    data.columns = [camel_to_snake(col) for col in data.columns]

    return data


def set_level(data: pd.DataFrame):
    """determine if the row applies to a country or region"""
    required_cols = ["region_code", "country_code"]
    cols = check_columns_exist(data, required_cols)
    if cols:
        data["level"] = np.where(
            data["region_code"] == data["country_code"], "country", "region"
        )

    return data


def get_age_boundries(group: str) -> tuple[int | None, int | None]:
    """Define upper and lower limits of age banding bases on age grouping column"""
    if group.startswith("Age"):
        age_part = group[3:]
        if "_" in age_part:  # Format: Age10_14
            lower, upper = age_part.split("_")
            return int(lower), int(upper)
        if "<" in age_part:  # Format: Age<18
            upper = age_part.replace("<", "")
            return 0, int(upper)
        if "+" in age_part:  # Format: Age80+
            lower = age_part.replace("+", "")
            return int(lower), None
    return None, None  # "ALL, AGEUnk"


def create_age_range(data: pd.DataFrame):
    """
    Assign upper and lower age ranges based on age grouping column
    using assigned function
    """
    cols = ["age_group"]
    valid_cols = check_columns_exist(data, cols)
    if valid_cols:
        data[["age_lower_limit", "age_upper_limit"]] = data["age_group"].apply(
            lambda x: pd.Series(get_age_boundries(x))
        )

    return data


def create_datasets(
    data: pd.DataFrame,
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """Create required datasets from transformed data"""

    # create country lookup dataset
    cols = ["country_code"]
    countries = get_unique_data(data, cols, ["country_code"])

    # create region lookup dataset
    cols = ["country_code", "region_code"]
    regions = get_unique_data(data, cols, ["region_code"])

    # create vaccine lookup dataset
    cols = ["vaccine_code"]
    vaccines = get_unique_data(data, cols, ["vaccine_code"])

    # create age lookup dataset
    cols = ["age_group", "age_lower_limit", "age_upper_limit"]
    ages = get_unique_data(data, cols, ["age_group"])

    # create national metrics dataset
    national = data[data["level"] == "country"].copy()
    national = national.drop(columns=["year", "week", "level", "region_code"])

    # create regional metrics dataset
    regional = data[data["level"] == "region"].copy()
    regional = regional.drop(columns=["year", "week", "level"])

    return (countries, regions, vaccines, ages, national, regional)


# @profile
def transform_chunk():
    """Runs transformation process for the National Case Death EU data"""

    print("\nTransforming EU Vaccine Tracker")

    available_files = file_check(get_dir("RAW_FOLDER", "eu"), "/vaccine-tracker*.json")

    if available_files:
        print("Importing data and creating new columns")
        for file in available_files:
            for i, chunk in enumerate(load_json_chunk(file, 50000)):
                print(f"Processing Chunk {i}")
                chunk = rename_cols(chunk)
                chunk = set_level(chunk)
                chunk = create_age_range(chunk)
                chunk = create_week_start_end(chunk, "year_week")

                print(f"Creating Final Datasets for Chunk {i}")

                (
                    countries_lookup,
                    regions_lookup,
                    vaccines_lookup,
                    ages_lookup,
                    national_data,
                    regional_data,
                ) = create_datasets(chunk)

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
                    "vt-country.json",
                    "vt-regions.json",
                    "vt-vaccines.json",
                    "vt-ages.json",
                    "vt-nat-data.json",
                    "vt-reg-data.json",
                ]

                first_chunk = i == 0

                print(f"Saving Chunk {i} {first_chunk}")
                for dataset, filename in zip(datasets, filenames):
                    save_chunk_to_json(dataset, filename, "eu", first_chunk)


# @profile
def transform_whole():
    """Runs transformation process for the National Case Death EU data"""

    print("\nTransforming EU Vaccine Tracker")

    file_path = get_dir("RAW_FOLDER", "eu")
    available_files = file_check(file_path, "/vaccine-tracker*.json")

    if available_files:
        for file in available_files:
            print("Importing data and creating new columns")
            data = load_json(file)
            data = rename_cols(data)
            data = set_level(data)
            data = create_age_range(data)
            data = create_week_start_end(data, "year_week")

            print("Creating Final Datasets")

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
                "vtw-country.json",
                "vtw-regions.json",
                "vtw-vaccines.json",
                "vtw-ages.json",
                "vtw-nat-data.json",
                "vtw-reg-data.json",
            ]

            save_to_json(datasets, filenames, "eu")


def transform():
    """run timing to defermine if full or chunk load/prosessing is best"""
    print("Run timing")
    print(datetime.datetime.now())
    transform_chunk()
    # whole_time = timeit.timeit("transform_whole()", globals=globals(), number=3)
    print(datetime.datetime.now())
    transform_whole()
    # chunked_time = timeit.timeit("transform_chunk()", globals=globals(), number=3)
    # print(datetime.datetime.now())

    # print(f"Whole file avg time: {whole_time/3:.3f}s")
    # print(f"Chunked time:   {chunked_time/3:.3f}s")

    # print("profiling")
    # lp = LineProfiler()
    # lp_wrapper = lp(transform_whole())
    # lp_wrapper()
    # lp.print_stats()

    # lp_wrapper = lp(transform_chunk())
    # lp_wrapper()
    # lp.print_stats()
