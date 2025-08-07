from zipfile import ZipFile, is_zipfile
from io import BytesIO

def save_file(data, filepath: str):  
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


def unzip_files(filepath: str):
    print(filepath)
    zip_files = [f for f in os.listdir(filepath) if f.endswith('.zip') and os.path.isfile(os.path.join(filepath, f))]
    #zip_files = os.listdir(filepath)
    if len(zip_files)> 0:
        print(zip_files)
        for zip_file in zipfiles:
            ZipFile.extract(zip_file)
            #try:
            #    ZipFile.extract(zip_file)
            #except: 
            #    Print(f"Error extracting zip file {zip_file}")
    else:
        print("no zip files found")

def check_for_files(filelist):
    zip_files = [f for f in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, f))]