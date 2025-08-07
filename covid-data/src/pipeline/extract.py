import os
import requests
import json
from zipfile import ZipFile, is_zipfile
from io import BytesIO

def get_save_dir():
    """Get target directory from env file"""
    return os.getenv('raw-folder')


def ensure_save_dir(path: str):
    """Ensure target directory exists."""
    os.makedirs(path, exist_ok=True)


def get_filename_from_endpoint(endpoint: str) -> str:
    """Convert endpoint to a clean filename."""
    base_part = endpoint.strip('/').split('/')[0]
    filename = base_part.replace('_', '-').lower()
    return filename


def fetch_data(url: str):
    """Fetch and validate JSON from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        content_type = response.headers.get('content-type', '').lower()
                                   
        if content_type == 'application/json':
            return response.json()  # Raises JSONDecodeError if invalid
        elif  content_type == 'application/zip':
            return response.content
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for {url}: {e}")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON at {url}: {e}")
    return None


def save_to_file(data, filepath: str):
    
    """Save JSON data to file with formatting."""
    try:
        if type(data) == list:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        elif is_zipfile(BytesIO(data)):
            with open(filepath, mode='wb') as f:
                f.write(data)
        print(f"Saved: {filepath}")              
    except IOError as e:
        print(f"Failed to write to {filepath}: {e}")

def unzip_files(filepath):
    print(filepath)
    #zip_files = [f for f in os.listdir(filepath) if f.endswith('.zip') and os.path.isfile(os.path.join(filepath, f))]
    zip_files = os.listdir(filepath)
    print(zip_files)

def process_endpoints(extract_config):
    """Main loop to fetch and save all endpoints."""
    for extract in extract_config:
        base_url, endpoints, folder = extract  
        root_folder = get_save_dir()
        full_dir = os.path.join(root_folder, folder)           
        ensure_save_dir(full_dir)

        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            filename = get_filename_from_endpoint(endpoint)
            filepath = os.path.join(full_dir, filename)

            if not os.path.exists(filepath):
                print(f"\nFetching: {url}")
                data = fetch_data(url)

                if data is not None:
                    save_to_file(data, filepath)
            else:
                print(f'{filename} already exists in target location skipping')
        
    unzip_files(root_folder)
