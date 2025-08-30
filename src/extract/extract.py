"""extract and process raw data files for all covid sources
ahead for transformation steps"""

import os
from io import BytesIO

import requests
from tqdm import tqdm

from utils import create_dir, get_dir, get_file, unzip_files


def get_filename_from_endpoint(endpoint: str) -> str:
    """Convert endpoint to a clean filename."""

    base_part = endpoint.strip("/").split("/")[0]
    filename = base_part.replace("_", "-").lower()
    # override for specific file
    if endpoint == "json/data.json":
        filename = "colour-key.json"
    elif "." not in filename:
        filename += ".json"

    return filename


def download_data(url: str, filepath: str, filename: str):
    """Gets data from requested URL and sabes data to
    specified locaiton with give filename. displays file
    download procgress in console
    """

    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0)) or None
        chunk_size = 8192
        data_buffer = BytesIO()

        with open(filepath, "wb") as file, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=f"Downloading {filename}",
        ) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    file.write(chunk)
                    data_buffer.write(chunk)
                    pbar.update(len(chunk))

    except requests.exceptions.RequestException as e:
        print(f"Request error for {url}: {e}")
    except IOError as e:
        print(f"Failed to write to {filepath}: {e}")


def process_endpoints(regions: list, env_vars: dict | None):
    """Main loop to fetch and save all endpoints."""

    print("###################################")
    print("Starting Extract")

    for region in regions:
        base_url, endpoints, folder = region
        print(f"\nDownloading {folder} files")

        # get the complete save path and create if it doesn't exist
        save_dir = get_dir("RAW_FOLDER", folder, env_vars)
        create_dir(save_dir)

        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            filename = get_filename_from_endpoint(endpoint)
            filepath = get_file(save_dir, filename)

            if not os.path.exists(filepath):
                download_data(url, filepath, filename)
            else:
                print(f"{filename} already exists")
        print(f"\nExtracting {folder} files")
        unzip_files(save_dir)

    print("\nFinished Extract")
