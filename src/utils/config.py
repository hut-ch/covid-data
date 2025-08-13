"""pipeline configuration data and settings"""

import dotenv


def get_set_config(file_path=".env"):
    """get and set environment variables
    for use by the rest of the job"""
    env_loc = dotenv.find_dotenv(file_path)
    if env_loc != "":
        dotenv.load_dotenv(env_loc)
    else:
        raise FileNotFoundError(
            f"Envirnonment file {file_path} not found \
                                please add to project! \
                                Try renaming {file_path}-sample file"
        )


def get_eu_details():
    """get endpoint details for EU region
    and retun as a list"""
    base_url = "https://opendata.ecdc.europa.eu/covid19/"
    endpoints = [
        "agecasesnational/json/",
        "COVID-19_VC_data_from_May_2024/json/data_v7.json",
        "hospitalicuadmissionrates/json/",
        "vaccine_tracker/json/",
        "movementindicators/json/",
        "movementindicatorsarchive/json/data.json",
        "movementindicatorsarchive2022/json/data.json",
        "nationalcasedeath/json/",
        "nationalcasedeath_archive/json/",
        "nationalcasedeath_eueea_daily_ei/json/",
        "subnationalcasedaily/json/",
        "subnationalcaseweekly/json/",
        "testing/json",
        "virusvariant/json/",
        "json/data.json",
    ]
    location = "eu"

    return [base_url, endpoints, location]


def get_uk_details():
    """get endpoint details for UK region
    and retun as a list"""
    base_url = (
        "https://archive.ukhsa-dashboard.data.gov.uk/coronavirus-dashboard/"  # noqa
    )
    endpoints = [
        "cases.zip",
        "deaths.zip",
        "healthcare.zip",
        "testing.zip",
        "vaccinations.zip",
    ]
    location = "uk"

    return [base_url, endpoints, location]


def get_details(location):
    """get endpoint details for all configuired
    regions and retun as a list"""
    if location == "eu":
        details = [get_eu_details()]
    elif location == "uk":
        details = [get_uk_details()]
    elif location == "all":
        eu = [get_eu_details()]
        uk = [get_uk_details()]
        details = eu + uk
    else:
        return None

    return details
