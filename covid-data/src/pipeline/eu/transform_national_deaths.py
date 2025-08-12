"""main transformation for National DEaths EU data"""

import pandas as pd
from pipeline.utils import file_check, get_dir


def national_deaths():
    """transform case death eu data"""
    nat_deaths = "nationalcasedeath.json"
    nat_deaths_arch = "nationalcasedeath-archive.json"
    nat_death_daily = "nationalcasedeath-eueea-daily-ei.json"

    file_list = [nat_deaths, nat_deaths_arch, nat_death_daily]
    file_path = get_dir("raw-folder", "eu")

    available_files = file_check(file_list, file_path)

    for file in available_files:
        data = pd.read_json(file)
        print(file)
        print(data.head)
