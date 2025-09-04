"""file and folder utilities for the pipeline"""

import glob
import json
import os
import time
from zipfile import BadZipFile, ZipFile, is_zipfile

import pandas as pd

from utils.config import get_variable
from utils.logs import get_logger

logger = get_logger(__name__)


def unzip_files(filepath: str):
    """
    Gets a list of .zip files from the given file path

    if there are zip filesP
        - For each fleP
            - Checks if it is a valid zip file
            - Unzips the contents if they haven't been extracted
    """
    zip_files = [
        f
        for f in os.listdir(filepath)
        if f.endswith(".zip") and os.path.isfile(os.path.join(filepath, f))
    ]

    if zip_files:
        logger.info("Extracting files from %s", filepath)
        for zip_file in zip_files:
            zip_path = get_file(filepath, zip_file)

            if is_zipfile(zip_path):
                try:
                    with ZipFile(zip_path, mode="r") as archive:
                        to_extract = [
                            f
                            for f in archive.namelist()
                            if not os.path.exists(os.path.join(filepath, f))
                        ]

                        if not to_extract:
                            logger.info("%s already extracted", zip_file)
                            continue

                        start_time = time.time()
                        last_log = start_time
                        extracted_count = 0
                        interval = 15

                        for member in to_extract:
                            archive.extract(member, filepath)
                            extracted_count += 1

                            now = time.time()
                            if now - last_log >= interval:
                                logger.info(
                                    "Extracting %s: %d/%d files extracted in %.1fs",
                                    zip_file,
                                    extracted_count,
                                    len(to_extract),
                                    now - start_time,
                                )
                                last_log = now

                        # Final summary
                        elapsed = time.time() - start_time
                        logger.info(
                            "Finished extracting %s: %d/%d files in %.1fs",
                            zip_file,
                            extracted_count,
                            len(to_extract),
                            elapsed,
                        )

                except BadZipFile:
                    logger.error("Error extracting zip file: %s", zip_file)
            else:
                logger.warning("%s is not a valid zip file", zip_file)


def get_dir(root: str, folder: str, env_vars: dict | None) -> str:
    """Get target directory from env file or the root"""
    root_folder = get_variable(root, env_vars)

    return get_file(root_folder, folder)


def get_file(filepath: str, file: str) -> str:
    """Gets file path"""
    return os.path.join(filepath, file)


def create_dir(path: str):
    """Ensure target directory exists."""
    os.makedirs(path, exist_ok=True)


def file_exists(filepath, file) -> bool:
    """Check if a file exists"""
    file_loc = get_file(filepath, file)

    return os.path.exists(file_loc)


def file_check(filepath: str, pattern: str):
    """Checks to see if files exist in given path matching pattern"""
    files = glob.glob(filepath + pattern)

    if not files:
        return None

    return files


def load_json(file: str) -> pd.DataFrame:
    """
    load json data and check the structure of the file
    if the data is wrappped in {"records": [...]}
        Create the datafram from records contents
    else
        Create Pandas dataframe
    """

    with open(file, "r", encoding="utf8") as f:
        raw_data = json.load(f)

    # check if data  is wrapped in {"records": [...]}
    if isinstance(raw_data, dict) and "records" in raw_data:
        data = pd.DataFrame(raw_data["records"])
    elif isinstance(raw_data, list):
        data = pd.DataFrame(raw_data)
    else:
        raise ValueError("Unsupported JSON structure")

    return data


def import_transformed_data(file: str) -> pd.DataFrame | None:
    """Try loading the json file depending on format"""
    try:
        data = pd.read_json(file)
        return data
    except ValueError:
        pass

    try:
        data = pd.read_json(file, lines=True)
        return data
    except ValueError as e:
        logger.error("Unable to load data from file %s %s", file, repr(e))
        return None


def save_to_json(datasets: list, file_names: list, folder: str, env_vars: dict | None):
    """
    Outputs the given Dataframes as json files into the
    cleansed-data folder with the given filename

    Args:
    datasets: Pandas Dataframes to be ouptut .
    file_names: filenames for each DatFrame to be output.
    folder: folder inside cleansed data to be saved

    Returns:
        A Pandas Dataframe for movement indicator data.
    """
    save_dir = get_dir("CLEANSED_FOLDER", folder, env_vars)
    create_dir(save_dir)

    for dataset, filename in zip(datasets, file_names):
        logger.info("Saving File %s", filename)

        file = get_file(save_dir, filename)
        dataset.reset_index(drop=True, inplace=True)
        dataset.to_json(file, date_unit="ns")


def save_chunk_to_json(
    data_chunk: pd.DataFrame,
    filename: str,
    folder: str,
    env_vars: dict | None,
    first_chunk=False,
):
    """
    Append a chunk of data to a JSON array in a file.
    Maintains valid JSON structure by handling commas and brackets properly.
    """
    logger.info("Saving chunk to file %s", filename)

    save_dir = get_dir("CLEANSED_FOLDER", folder, env_vars)
    path = get_file(save_dir, filename)

    mode = "w" if first_chunk else "a"

    data_chunk.to_json(path, mode=mode, orient="records", lines=True, date_unit="ns")
