import pandas as pd
from piepline.utils import check_for_files



def movement_indicators(file_path):
    file_list = [
        'movementindicators.json',
        'movementindicatorsarchive.json'
    ]

    file_list = [file_path+f for f in file_list]

    if check_for_files(file_list):
        data = pd.read_json(file_path)
        data_archive = pd.read_json(file_path)
        
    else:
        print(f"Required files not found {file_list}")

    return None


def vaccine_tracker():
    file_list = [
        'vaccine_tracker.json',
    ]
    return None


def nationalcasedeath():
    file_list = [
        'nationalcasedeath.json',
        'nationalcasedeath_archive.json',
        'nationalcasedeath_eueea_daily_ei.json'
    ]

    return None
        

def otherdata():        
    file_list = [
        'COVID-19_VC_data_from_September_2023.json',
    ]
        
        
 