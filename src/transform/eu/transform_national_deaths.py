"""main transformation for National DEaths EU data"""

import pandas as pd

from utils import file_check, get_dir


def national_deaths():
    """transform case death eu data"""
    file_path = get_dir("raw-folder", "eu")
    available_files = file_check(file_path, "/nationalcasedeath*.json")

    for file in available_files:
        data = pd.read_json(file)
        print(file)
        print(data.head)
