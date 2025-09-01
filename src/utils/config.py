"""pipeline configuration data and settings"""

import os

import dotenv

from utils.logs import get_logger

logger = get_logger(__name__)


def get_set_config(file_path=".env"):
    """get and set environment variables
    for use by the rest of the job"""
    env_loc = dotenv.find_dotenv(file_path)
    if env_loc != "":
        dotenv.load_dotenv(env_loc)
        logger.info("Environment variables loaded from %s", file_path)

        # pick only folder + DB_ vars
        # this is for airflow DAG runs if just running the
        # scripts manually this is ignered and the vars are read directly
        selected = {
            k: v
            for k, v in os.environ.items()
            if k in {"DATA_FOLDER", "RAW_FOLDER", "CLEANSED_FOLDER"}
            or k.startswith("DB_")
        }

        logger.info("Config passed to XCom: %s", list(selected.keys()))
        return selected

    raise FileNotFoundError(
        f"""Envirnonment file {file_path} not found please add to project!
        Try renaming {file_path}-example"""
    )


def get_variable(name: str, env_vars: dict | None) -> str:
    """get the environement variable value or the value form the passed in dictionary"""
    if env_vars is not None:
        value = env_vars[name]  # raises KeyError if missing
    else:
        value = os.getenv(name)
        if value is None:
            raise KeyError(
                f"{name} not found in vars dictionary or environment variables"
            )
    return value


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
    base_url = "https://archive.ukhsa-dashboard.data.gov.uk/coronavirus-dashboard/"
    endpoints = [
        "cases.zip",
        "deaths.zip",
        "healthcare.zip",
        "testing.zip",
        "vaccinations.zip",
    ]
    location = "uk"

    return [base_url, endpoints, location]


def get_country_codes():
    """get country codes lookup"""
    base_url = (
        "https://raw.githubusercontent.com/datasets/country-list/refs/heads/main/"
    )
    endpoints = ["data.csv"]
    location = "lookup"

    return [base_url, endpoints, location]


def get_details(location):
    """get endpoint details for all configuired
    regions and retun as a list"""
    if location == "eu":
        details = [get_eu_details(), get_country_codes()]
    elif location == "uk":
        details = [get_uk_details(), get_country_codes()]
    elif location == "all":
        details = [get_eu_details(), get_uk_details(), get_country_codes()]
    else:
        return None

    return details
