"""extract and process raw data files for all covid sources
ahead for transformation steps"""

import os
from io import BytesIO

import requests
from pipeline.utils import create_dir, get_dir, get_file, unzip_files
from tqdm import tqdm


def get_filename_from_endpoint(endpoint: str) -> str:
    """Convert endpoint to a clean filename."""
    base_part = endpoint.strip("/").split("/")[0]
    filename = base_part.replace("_", "-").lower()
    if "." not in filename:
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
            ascii=True,
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


def process_endpoints(regions: list):
    """Main loop to fetch and save all endpoints."""
    for region in regions:
        base_url, endpoints, folder = region
        # get the complete save path and create if it doesn't exist
        save_dir = get_dir("raw-folder", folder)
        create_dir(save_dir)

        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            filename = get_filename_from_endpoint(endpoint)
            filepath = get_file(save_dir, filename)

            if not os.path.exists(filepath):
                download_data(url, filepath, filename)
                print(f"Saved as {filepath}")
            else:
                print(f"{filename} already exists in target location skipping")

        unzip_files(save_dir)
