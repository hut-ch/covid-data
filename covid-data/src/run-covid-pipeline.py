# runs the main pipeling to extract codiv daat fro uk and eu sources if it doesnt already exist and tranfrom the data ready to be stored in final format
from pipeline_utils import credentials
from pipeline import extract

def setup_values():
    # setup environment and extract details
    credentials.get_env_config('covid-data.env')


def process_uk_data():
    endpoints = credentials.get_details('uk')
    print(endpoints)
    #extract(endpoints)


def process_eu_data():
    endpoints = credentials.get_details('eu')
    #extract(endpoints)
    print(endpoints)


def process_all_data():
    process_uk_data()
    process_eu_data()    

if __name__ == "__main__":
    process_all_data()