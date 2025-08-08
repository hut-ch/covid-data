"""main transformation for EU data"""

import pandas as pd
from pipeline.utils import file_check, get_dir, get_file


def movement_indicators():
    """transform movement indicators eu data"""
    move_ind = "movementindicators.json"
    move_ind_arch = "movementindicatorsarchive.json"

    file_list = [move_ind, move_ind_arch]
    file_path = get_dir("raw-folder", "eu")

    available_files = file_check(file_list, file_path)

    if move_ind in available_files:
        file = get_file(file_path, move_ind)
        data = pd.read_json(file)

        print(data.describe())
        print(data.shape)
        print(data.head())
    else:
        print(f"Required files {file} not found skipping tranformation")

    if move_ind_arch in available_files:
        file = get_file(file_path, move_ind_arch)
        data_archive = pd.read_json(file)

        print(data_archive.describe())
        print(data_archive.shape)
        print(data_archive.head())
    else:
        print(f"Required files {file} not found skipping tranformation")


def vaccine_tracker(file_path):
    """transform vaccine tracker eu data"""
    file_list = [
        "vaccine_tracker.json",
    ]

    file_list = [file_path + f for f in file_list]


def nationalcasedeath(file_path):
    """transform mcase death eu data"""
    file_list = [
        "nationalcasedeath.json",
        "nationalcasedeath_archive.json",
        "nationalcasedeath_eueea_daily_ei.json",
    ]

    file_list = [file_path + f for f in file_list]


def otherdata(file_path):
    """transform other eu data"""
    file_list = [
        "COVID-19_VC_data_from_September_2023.json",
    ]

    file_list = [file_path + f for f in file_list]
