"""file and folder utilities for the pipeline"""

import glob
import json
import os
from io import BytesIO
from zipfile import BadZipFile, ZipFile, is_zipfile

import pandas as pd
from tqdm import tqdm


def is_json(data):
    """probably can be removed:check is dfata is json format"""
    try:
        json.dumps(data)
        return True
    except TypeError:
        return False


def save_file(data, filepath: str):
    """probably can be removed: save a json or zip file"""
    try:
        if is_json(data):
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        elif is_zipfile(BytesIO(data)):
            with open(filepath, mode="wb") as f:
                f.write(data)
        print(f"Saved: {filepath}")
    except IOError as e:
        print(f"Failed to write to {filepath}: {e}")


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
        for zip_file in zip_files:
            zip_path = get_file(filepath, zip_file)

            if is_zipfile(zip_path):
                try:
                    with ZipFile(zip_path, mode="r") as archive:
                        not_extracted = [
                            f
                            for f in archive.namelist()
                            if not os.path.exists(os.path.join(filepath, f))
                        ]

                        if not_extracted:
                            for member in tqdm(
                                not_extracted,
                                desc=f"Extracting {zip_file}",
                                unit="file",
                            ):
                                archive.extract(member, filepath)
                        else:
                            print(f"{zip_file} already extracted")
                except BadZipFile:
                    print(f"Error extracting zip file: {zip_file}")
            else:
                print(f"{zip_file} is not a valid zip file")


def get_dir(root, folder):
    """Get target directory from env file"""
    root_folder = os.getenv(root)
    return get_file(root_folder, folder)


def get_file(filepath, file):
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


def save_to_json(datasets, file_names, folder):
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
    save_dir = get_dir("cleansed-data", folder)
    create_dir(save_dir)

    for dataset, filename in zip(datasets, file_names):
        file = get_file(save_dir, filename)
        dataset.reset_index(drop=True, inplace=True)
        dataset.to_json(file)


def save_chunk_to_json(data_chunk: pd.DataFrame, filename, folder, first_chunk=False):
    """
    Append a chunk of data to a JSON array in a file.
    Maintains valid JSON structure by handling commas and brackets properly.
    """
    save_dir = get_dir("cleansed-data", folder)
    path = get_file(save_dir, filename)

    mode = "w" if first_chunk else "a"

    data_chunk.to_json(path, mode=mode, orient="records", lines=True)

    # with open(path, mode, encoding="utf-8") as f:
    #    if first_chunk:
    #       f.write("[")
    #  else:
    #     f.write(",\n")  # separate chunks with commas

    # json.dump(data_chunk, f, ensure_ascii=False)
