"""Runs all transformations for EU data"""

from transform.eu import mi_transform, nd_transform, vt_transform
from utils import get_variable


def transform_all(env_vars: dict | None):
    "Run all transformations"

    transform_eu(env_vars)
    transform_uk(env_vars)


def transform_eu(env_vars: dict | None):
    "Run EU transformations"

    print("###################################")
    print("Starting EU Transformation")

    mi_transform(env_vars)
    nd_transform(env_vars)
    vt_transform(env_vars)

    print("\nFinished EU Transformation")


def transform_uk(env_vars: dict | None):
    "Run UK transformations"

    print("###################################")
    print("Starting UK Transformation")
    print(get_variable("RAW_FOLDER", env_vars))
    print(get_variable("CLEANSED_FOLDER", env_vars))
    print("\nFinished UK Transformation")
