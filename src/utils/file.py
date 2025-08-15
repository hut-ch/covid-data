"""file and folder utilities for the pipeline"""

import glob
import json
import os
from io import BytesIO
from zipfile import BadZipFile, ZipFile, is_zipfile


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
    """gets a list of .zip files from the given file path
    if there are zip files then for each fle checks if it is
    a valid zip file and unzips the cxontents if it hasn't
    already been extracted
    """
    zip_files = [
        f
        for f in os.listdir(filepath)
        if f.endswith(".zip") and os.path.isfile(os.path.join(filepath, f))
    ]
    # zip_files = os.listdir(filepath)
    if len(zip_files) > 0:
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
                            print(f"Extracting file: {zip_file}")
                            archive.extractall(filepath, not_extracted)
                            print(f"File extracted: {zip_file}")
                        else:
                            print(f"{zip_file} already extracted, skipping")
                except BadZipFile:
                    print(f"Error extracting zip file: {zip_file}")
            else:
                print(f"{zip_file} is not a valid zip file, skipping")
    else:
        print("No zip files found")


def get_dir(root, folder):
    """Get target directory from env file"""
    root_folder = os.getenv(root)
    return os.path.join(root_folder, folder)


def get_file(filepath, file):
    """Gets file path"""
    return os.path.join(filepath, file)


def create_dir(path: str):
    """Ensure target directory exists."""
    os.makedirs(path, exist_ok=True)


def file_check(filepath: str, pattern: str):
    """Checks to see if files exist in given path matching pattern"""
    files = glob.glob(filepath + pattern)

    if not files:
        return None

    return files
