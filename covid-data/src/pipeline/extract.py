import os
import requests
import json
from pipeline.utils import save_file, unzip_files


def get_save_dir(folder):
    """Get target directory from env file"""
    root_folder = os.getenv('raw-folder')
    return os.path.join(root_folder, folder) 


def ensure_save_dir(path: str):
    """Ensure target directory exists."""
    os.makedirs(path, exist_ok=True)


def get_filename_from_endpoint(endpoint: str) -> str:
    """Convert endpoint to a clean filename."""
    base_part = endpoint.strip('/').split('/')[0]
    filename = base_part.replace('_', '-').lower()
    if '.' not in filename:
        filename += '.json'
    
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


def process_endpoints(extract_config: list):
    """Main loop to fetch and save all endpoints."""
    for extract in extract_config:
        base_url, endpoints, folder = extract  
        save_dir =  get_save_dir(folder) # get the complete save path       
        ensure_save_dir(save_dir) # create save folder if is doesn't exist

        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            filename = get_filename_from_endpoint(endpoint)
            filepath = os.path.join(save_dir, filename)

            if not os.path.exists(filepath):
                print(f"\nFetching: {url}")
                data = fetch_data(url)

                if data is not None:
                    save_file(data, filepath)
            else:
                print(f'{filename} already exists in target location skipping')
        
    unzip_files(os.getenv('raw-folder'))
