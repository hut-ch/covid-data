import dotenv, os


# get environment variables to use by the rest of the job
def get_env_config(file_path='covid-data.env'):
    try:
        load_dotenv(file_path)   
    except FileNotFoundError as e:
        print(f"covid-data.env not found - {e}" )
        raise e

    print(os.getenv('db-user'))

def get_eu_details():
    base_url = 'https://opendata.ecdc.europa.eu/covid19/'

    endpoints = [
        'COVID-19_VC_data_from_September_2023/json/data_v7.json',
        'vaccine_tracker/json/',
        'movementindicators/json/',
        'movementindicatorsarchive/json/data.json',
        'nationalcasedeath/json/',
        'nationalcasedeath_archive/json/',
        'nationalcasedeath_eueea_daily_ei/json/'
    ]

    location = 'eu'

    return [base_url, endpoints, location]

def get_uk_details():
    base_url = 'https://opendata.ecdc.europa.eu/covid19/'

    endpoints = [
        'COVID-19_VC_data_from_September_2023/json/data_v7.json',
        'vaccine_tracker/json/',
        'movementindicators/json/',
        'movementindicatorsarchive/json/data.json',
        'nationalcasedeath/json/',
        'nationalcasedeath_archive/json/',
        'nationalcasedeath_eueea_daily_ei/json/'
    ]

    location = 'uk'

    return [base_url, endpoints, location]

def get_details(location):
    if location == 'eu':
        details = get_eu_details()
    elif location =='uk':
        details = get_uk_details()
    else: 
        raise Exception('Invalid location provided')
    
    return details