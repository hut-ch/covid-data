"""extract and process raw data files for all covid sources
ahead for transformation steps"""

import os
import time

import requests

from utils import create_dir, get_dir, get_file, get_logger, unzip_files

logger = get_logger(__name__)


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
        chunk_size = 1024 * 1024  # 1 MB

        start_time = time.time()
        last_log = start_time
        downloaded = 0
        interval = 10

        with open(filepath, "wb") as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue

                file.write(chunk)
                downloaded += len(chunk)

                # log every <interval> seconds downlad progress
                now = time.time()
                if now - last_log >= interval:
                    elapsed = now - start_time
                    if total_size:
                        logger.info(
                            "Downloading %s: %.1f%% (%s/%s) in %.1fs (%.2f MB/s)",
                            filename,
                            downloaded / total_size * 100,
                            f"{downloaded/1024/1024:.1f}MB",
                            f"{total_size/1024/1024:.1f}MB",
                            elapsed,
                            downloaded / elapsed if elapsed > 0 else 0 / 1024 / 1024,
                        )
                    else:
                        logger.info(
                            "Downloading %s: %s downloaded in %.1fs (%.2f MB/s)",
                            filename,
                            f"{downloaded/1024/1024:.1f}MB",
                            elapsed,
                            downloaded / elapsed if elapsed > 0 else 0 / 1024 / 1024,
                        )
                    last_log = now

        # Final summary
        elapsed = time.time() - start_time
        logger.info(
            "Finished downloading %s: %s in %.1fs (%.2f MB/s)",
            filename,
            f"{downloaded/1024/1024:.1f}MB",
            elapsed,
            (downloaded / elapsed) / (1024 * 1024),
        )

    except requests.exceptions.RequestException as e:
        logger.error("Request error for %s: %s", url, e)
    except IOError as e:
        logger.error("Failed to write to %s: %s", filepath, e)


def process_endpoints(locations: list, env_vars: dict | None):
    """Main loop to fetch and save all endpoints."""

    logger.info("Starting Extract")

    for location in locations:
        base_url, endpoints, folder = location
        logger.info("Downloading %s files", folder)

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
                logger.info("%s already exists", filename)
        unzip_files(save_dir)

    logger.info("Finished Extract")
