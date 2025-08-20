"""Main transformation for Vaccine Tracker EU data"""

"""main transformation for National DEaths EU data"""

import json

import os
import pandas as pd
import numpy as np
import re
import ijson

from utils import (
    check_columns_exist,
    combine_data,
    create_week_start_end,
    file_check,
    get_dir,
    get_unique_data,
    is_subset,
    merge_rows,
    save_to_json,
)

def load_json_chunk(file: str, chunk_size: int = 1000):
    """
    Stream and load JSON records from a large JSON file in chunks.
    The JSON must be a dict with a 'records' key, whose value is a large array.

    Yields:
        pd.DataFrame: DataFrame chunk of specified size
    """
    with open(file, 'r', encoding='utf-8') as f:
        objects = ijson.items(f, 'records.item')
        buffer = []

        for obj in objects:
            buffer.append(obj)
            if len(buffer) >= chunk_size:
                yield pd.DataFrame(buffer)
                buffer = []

        if buffer:
            yield pd.DataFrame(buffer)         
            
def output_json_chunk(path, data_chunk, first_chunk=False):
    """
    Append a chunk of data to a JSON array in a file.
    Maintains valid JSON structure by handling commas and brackets properly.
    """
    mode = 'w' if first_chunk else 'a'
    with open(path, mode, encoding='utf-8') as f:
        if first_chunk:
            f.write('[')
        else:
            f.write(',\n')  # separate chunks with commas

        json.dump(data_chunk, f, ensure_ascii=False)

        
def finalize_json_file(path):
    """
    Finalizes a streamed JSON array file by closing it with a ']'.
    """
    with open(path, 'a', encoding='utf-8') as f:
        f.write(']')
            
def camel_to_snake(name):
    # Convert 'CamelCase' or 'camelCase' to 'camel_case'
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)     # Handle first capital in word
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)     # Handle adjacent capitals
    return name.lower()   


def rename_cols(data: pd.DataFrame) -> pd.DataFrame:
    """
    Import for most common format of movment indicator data from source
    json file into Pandas Dataframe then rename columns
    """
    data.rename(
        columns={
            "YearWeekISO": "year_week",
            "ReportingCountry": "country",
            "TargetGroup": "age_group",
        },
        inplace=True,
    )
    data.columns = [camel_to_snake(col) for col in data.columns]
    
    return data


def set_level(data: pd.DataFrame): 
    data["level"] = np.where(data["region"] == data["country"], "country","region")
    
    return data


def get_age_boundries(group: str):
    if group.startswith("Age"):
        age_part = group[3:]
        if "_" in age_part:  # Format: Age10_14
            lower, upper = age_part.split("_")
            return int(lower), int(upper)
        elif "<" in age_part:  # Format: Age<18
            upper = int(age_part.replace("<", ""))
            return 0, upper
        elif "+" in age_part:  # Format: Age80+
            lower = int(age_part.replace("+", ""))
            return lower, None
    return None, None  # "ALL, UnkAge"

def create_age_range(data: pd.DataFrame):
    data[['age_lower', 'age_upper']] = data['age_group'].apply(lambda x: pd.Series(get_age_boundries(x)))
    
    return data
    
def create_country_columns(data: pd.DataFrame)):
    return data

def create_datasets(data: pd.DataFrame):
    
    return data

def transform():
    """Runs transformation process for the National Case Death EU data"""

    print("\nTransforming EU National Case Deaths")

    file_path = get_dir("raw-folder", "eu")
    available_files = file_check(file_path, "/nationalcasedeath*.json")
    full_data = pd.DataFrame()

    if available_files:
        print("Importing data and creating new columns")
        
        for chunk in load_json_chunked(file, chunk_size=1000):
            data = rename_cols(chunk)
            data = set_level(data)
            data = create_age_range(data)
            
            ### create start and end week
            ### create_country_columns()
            ### create_datasets()

            ### output datasets()
            
            
            print(data.head())
            

