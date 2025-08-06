import os
import requests
import json
from pipeline_utils import eu_config, uk_config

def get_save_dir():
    """Get target directory from env file"""
    return os.getenv('raw-folder')


def ensure_save_dir(path: str):
    """Ensure target directory exists."""
    os.makedirs(path, exist_ok=True)


def get_filename_from_endpoint(endpoint: str) -> str:
    """Convert endpoint to a clean filename."""
    base_part = endpoint.strip('/').split('/')[0]
    filename = base_part.replace('_', '-').lower() + '.json'
    return 


def fetch_json(url: str):
    """Fetch and validate JSON from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()  # Raises JSONDecodeError if invalid
    except requests.exceptions.RequestException as e:
        print(f"Request error for {url}: {e}")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON at {url}: {e}")
    return None


def save_json_to_file(data, filepath: str):
    """Save JSON data to file with formatting."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved: {filepath}")
    except IOError as e:
        print(f"Failed to write to {filepath}: {e}")


def process_endpoints(extract_config: list):
    """Main loop to fetch and save all endpoints."""
    for extract in extract_config:
        
        base_url = extract[0]
        endpoints = extract[1]
        save_dir = endpoint[2]
        
        root_dir = get_save_dir()
        ensure_save_dir(save_dir)

        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            filename = get_filename_from_endpoint(endpoint)
            filepath = os.path.join(root_dir, save_dir, filename)

            print(f"\nFetching: {url} and saving to {filepath}")
            data = fetch_json(url)

            if data is not None:
                save_json_to_file(data, filepath)
