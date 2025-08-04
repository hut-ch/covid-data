import os
from dotenv import load_dotenv
import requests
import json


BASE_URL = 'https://opendata.ecdc.europa.eu/covid19/'

ENDPOINTS = [
    'COVID-19_VC_data_from_September_2023/json/data_v7.json',
    'vaccine_tracker/json/',
    'movementindicators/json/',
    'movementindicatorsarchive/json/data.json',
    'nationalcasedeath/json/',
    'nationalcasedeath_archive/json/',
    'nationalcasedeath_eueea_daily_ei/json/'
]

def get_save_dir():
    """Get target directory from env file"""
    load_dotenv('../project.env')    
    file_dir = os.getenv('download_folder')
    return file_dir

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

def process_endpoints(endpoints, base_url):
    """Main loop to fetch and save all endpoints."""
    save_dir = get_save_dir()
    ensure_save_dir(save_dir)

    for endpoint in endpoints:
        url = f"{base_url}{endpoint}"
        filename = get_filename_from_endpoint(endpoint)
        filepath = os.path.join(save_dir, filename)

        print(f"\nFetching: {url}")
        data = fetch_json(url)

        if data is not None:
            save_json_to_file(data, filepath)

# Run the script
if __name__ == "__main__":
    process_endpoints(ENDPOINTS, BASE_URL)
    