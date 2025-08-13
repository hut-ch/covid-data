"""Main transformation for Vaccine Tracker EU data"""

from utils import file_check, get_dir


def vaccine_tracker():
    """transform vaccine tracker eu data"""
    vacc_track = "vaccine-tracker.json"

    file_list = [vacc_track]
    file_path = get_dir("raw-folder", "eu")

    available_files = file_check(file_list, file_path)
    print(available_files)
