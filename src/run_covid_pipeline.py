"""runs the main pipeline to extract covid data for uk and eu
sources if it doesn't already exist and tranforms the data
ready to be stored in final format"""

from extract import extract
from transform.eu import transform_movement_indicators as mi
from utils import get_details, get_set_config


def setup_values():
    """setup environment and extract details"""
    get_set_config(".env")


def process_uk_data():
    """run just uk covid data pipelins"""
    endpoints = get_details("uk")
    extract.process_endpoints(endpoints)


def process_eu_data():
    """run just eu covid data pipelins"""
    endpoints = get_details("eu")
    extract.process_endpoints(endpoints)


def process_all_data():
    """run all region covid data pipelines"""
    endpoints = get_details("all")
    extract.process_endpoints(endpoints)
    mi.transform()


if __name__ == "__main__":
    setup_values()
    process_all_data()
