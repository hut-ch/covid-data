"""runs the main pipeline to extract covid data for uk and eu
sources if it doesn't already exist and tranforms the data
ready to be stored in final format"""

import extract
import load
import transform
from utils import get_details, get_set_config


def setup_values():
    """setup environment and extract details"""
    get_set_config(".env")


def process_uk_data(env_vars: dict | None):
    """run just uk covid data pipeline"""
    endpoints = get_details("uk")
    extract.process_endpoints(endpoints, env_vars)
    transform.transform_uk(env_vars)
    load.create_schema(env_vars)
    load.load_uk(env_vars)


def process_eu_data(env_vars: dict | None):
    """run just eu covid data pipeline"""
    endpoints = get_details("eu")
    extract.process_endpoints(endpoints, env_vars)
    transform.transform_eu(env_vars)
    load.create_schema(env_vars)
    load.load_eu(env_vars, refresh=True)


def process_all_data(env_vars: dict | None):
    """run all region covid data pipelines"""
    endpoints = get_details("all")
    extract.process_endpoints(endpoints, env_vars)
    transform.transform_all(env_vars)
    load.create_schema(env_vars)
    load.load_all(env_vars)


if __name__ == "__main__":
    setup_values()
    process_all_data(None)
