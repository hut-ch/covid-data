# runs the main pipeling to extract codiv daat fro uk and eu sources if it doesnt already exist and tranfrom the data ready to be stored in final format
from pipeline.utils import get_details, get_env_config
from pipeline import extract

def setup_values():
    # setup environment and extract details
    get_env_config('covid-data.env')
    return 


def process_uk_data():
    endpoints = get_details('uk')
    extract.process_endpoints(endpoints)


def process_eu_data():
    endpoints = get_details('eu')
    extract.process_endpoints(endpoints)
    


def process_all_data():
    endpoints = get_details('all')
    extract.process_endpoints(endpoints)  


if __name__ == "__main__":
    setup_values()
    process_all_data()


# try this to see if it prints a function
# print(process_uk_data)
# or 
# print(process_uk_data.to_string())
# or
# print(repr(process_uk_data))
# or
# __string()__

# could crete the enpoints as a class and use that maybe