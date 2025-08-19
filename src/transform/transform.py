"""Runs all transformations for EU data"""

from transform.eu import mi_transform, nd_transform


def transform_all():
    "Run all transformations"

    transform_eu()
    transform_uk()


def transform_eu():
    "Run EU transformations"

    print("###################################")
    print("Starting EU Transformation")

    mi_transform()
    nd_transform()

    print("\nFinished EU Transformation")


def transform_uk():
    "Run UK transformations"

    print("###################################")
    print("Starting UK Transformation")

    print("\nFinished UK Transformation")
