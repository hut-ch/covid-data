""" runs the main pipeline to extract covid data for uk and eu
sources if it doesn't already exist and tranforms the data
ready to be stored in final format """

import pipeline.eu.transform as eu_transform
from pipeline import extract
from pipeline.utils import get_details, get_set_config


def setup_values():
    """setup environment and extract details"""
    get_set_config("covid-data.env")


def process_uk_data():
    """run just uk covid data pipelins"""
    endpoints = get_details("uk")
    extract.process_endpoints(endpoints)


def process_eu_data():
    """run just eu covid data pipelins"""
    endpoints = get_details("eu")
    extract.process_endpoints(endpoints)


def process_all_data():
    """run all region covid data pipelins"""
    endpoints = get_details("all")
    extract.process_endpoints(endpoints)
    eu_transform.movement_indicators()


if __name__ == "__main__":
    setup_values()
    process_all_data()
