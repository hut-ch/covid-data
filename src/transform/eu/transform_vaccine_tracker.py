"""Main transformation for Vaccine Tracker EU data"""

import datetime
import timeit

import ijson
import numpy as np
import pandas as pd

from utils import (  # combine_data, , is_subset, merge_rows
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
            "ReportingCountry": "country",
            "TargetGroup": "age_group",
        },
        inplace=True,
    )
    data.columns = [camel_to_snake(col) for col in data.columns]

    return data


def set_level(data: pd.DataFrame):
    """determine if the row applies to a country or region"""
    required_cols = ["region", "country"]
    cols = check_columns_exist(data, required_cols)
    if cols:
        data["level"] = np.where(data["region"] == data["country"], "country", "region")

    return data


def get_age_boundries(group: str) -> tuple[int | None, int | None]:
    """Define upper and lower limits of age banding bases on age grouping column"""
    if group.startswith("Age"):
        age_part = group[3:]
        if "_" in age_part:  # Format: Age10_14
            lower, upper = age_part.split("_")
            return int(lower), int(upper)
        if "<" in age_part:  # Format: Age<18
            upper = int(age_part.replace("<", ""))
            return 0, upper
        if "+" in age_part:  # Format: Age80+
            lower = int(age_part.replace("+", ""))
            return lower, None
    return None, None  # "ALL, AGEUnk"


def create_age_range(data: pd.DataFrame):
    """
    Assign upper and lower age ranges based on age grouping column
    using assigned function
    """
    cols = ["age_group"]
    cols = check_columns_exist(data, cols)
    if cols:
        data[["age_lower", "age_upper"]] = data["age_group"].apply(
            lambda x: pd.Series(get_age_boundries(x))
        )

    return data


def create_datasets(
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create required datasets from transformed data"""

    # create country lookup dataset
    cols = ["country"]
    countries = get_unique_data(data, cols, ["country"])

    # create region lookup dataset
    cols = ["country", "region"]
    regions = get_unique_data(data, cols, ["region"])

    # create region lookup dataset
    cols = ["vaccine"]
    vaccines = get_unique_data(data, cols, ["vaccine"])
    # .to_json(orient="records", lines=True)

    # create national metrics dataset
    national = data[data["level"] == "country"].copy()
    national = national.drop(columns=["year", "week", "level", "region"])

    # create regional metrics dataset
    regional = data[data["level"] == "region"].copy()
    regional = regional.drop(columns=["year", "week", "level"])

    return (countries, regions, vaccines, national, regional)


def transform_chunk():
    """Runs transformation process for the National Case Death EU data"""

    print("\nTransforming EU Vaccine Tracker")

    file_path = get_dir("raw-folder", "eu")
    available_files = file_check(file_path, "/vaccine-tracker*.json")

    if available_files:
        print("Importing data and creating new columns")
        for file in available_files:
            for i, chunk in enumerate(load_json_chunk(file, 50000)):
                print(f"Processing Chunk {i}")
                data = rename_cols(chunk)
                data = set_level(data)
                data = create_age_range(data)
                data = create_week_start_end(data, "year_week")

                print(f"Creating Final Datasets for Chunk {i}")

                (
                    countries_lookup,
                    regions_lookup,
                    vaccines_lookup,
                    national_data,
                    regional_data,
                ) = create_datasets(data)

                # output datasets
                datasets = [
                    countries_lookup,
                    regions_lookup,
                    vaccines_lookup,
                    national_data,
                    regional_data,
                ]
                filenames = [
                    "vt-countries.json",
                    "vt-regions.json",
                    "vt-vaccines.json",
                    "vt-national-data.json",
                    "vt-regional-data.json",
                ]

                first_chunk = 0 in i

                print(f"Saving Chunk {i} {first_chunk}")
                for dataset, filename in zip(datasets, filenames):
                    save_chunk_to_json(dataset, filename, "eu", first_chunk)


def transform_whole():
    """Runs transformation process for the National Case Death EU data"""

    print("\nTransforming EU Vaccine Tracker")

    file_path = get_dir("raw-folder", "eu")
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
                national_data,
                regional_data,
            ) = create_datasets(data)

            # output datasets
            datasets = [
                countries_lookup,
                regions_lookup,
                vaccines_lookup,
                national_data,
                regional_data,
            ]
            filenames = [
                "vtw-countries.json",
                "vtw-regions.json",
                "vtw-vaccines.json",
                "vtw-national-data.json",
                "vtw-regional-data.json",
            ]

            save_to_json(datasets, filenames, "eu")


def transform():
    """run timing to defermine if full or chunk load/prosessing is best"""
    print("Run timing")
    print(datetime.datetime.now())
    whole_time = timeit.timeit("transform_whole()", globals=globals(), number=3)
    print(datetime.datetime.now())
    chunked_time = timeit.timeit("transform_chunk()", globals=globals(), number=3)
    print(datetime.datetime.now())

    print(f"Whole file avg time: {whole_time/3:.3f}s")
    print(f"Chunked time:   {chunked_time/3:.3f}s")
