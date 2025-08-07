import os
import dotenv

# get environment variables to use by the rest of the job
def get_env_config(file_path='covid-data.env'):
    env_loc = dotenv.find_dotenv(file_path) 
    if env_loc != '':
        dotenv.load_dotenv(env_loc) 
    else:  
         raise FileNotFoundError(f"Envirnment file {file_path} not found please add to project! \
                                Try renaming {file_path}-sample file")        


def get_eu_details():
    base_url = 'https://opendata.ecdc.europa.eu/covid19/'

    endpoints = [
        #'COVID-19_VC_data_from_September_2023/json/data_v7.json',
        #'vaccine_tracker/json/',
        #'movementindicators/json/',
        #'movementindicatorsarchive/json/data.json',
        #'nationalcasedeath/json/',
        #'nationalcasedeath_archive/json/',
        #'nationalcasedeath_eueea_daily_ei/json/'
    ]

    location = 'eu'

    return [base_url, endpoints, location]

def get_uk_details():
    base_url = 'https://archive.ukhsa-dashboard.data.gov.uk/coronavirus-dashboard/'

    endpoints = [
        #'cases.zip',
        #'deaths.zip',
        'healthcare.zip',
        #'testing.zip',
        #'vaccinations.zip'
    ]

    location = 'uk'

    return [base_url, endpoints, location]

def get_details(location):
    if location == 'eu':
        details = [get_eu_details()]
    elif location =='uk':
        details = [get_uk_details()]
    elif location == 'all':
        eu = [get_eu_details()]
        uk = [get_uk_details()]
        details = eu + uk
    else: 
        raise Exception('Invalid location provided')
    
    return details
