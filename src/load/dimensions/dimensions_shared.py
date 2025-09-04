"""Manage EU specific Dimensions"""

from datetime import date, datetime

import pandas as pd
from sqlalchemy import engine

from load.dw import maintain_table
from utils import get_db_engine, get_logger, get_variable, save_to_json

logger = get_logger(__name__)


def maintain_shared_dims(env_vars: dict | None):
    "Run EU loads"

    logger.info("Starting Shared Dimension Load")
    db_engine = get_db_engine(env_vars)

    dim_date(db_engine=db_engine, start_date=None, num_years=-10, env_vars=env_vars)


def dim_date(
    db_engine: engine.Engine,
    start_date: date | None,
    num_years: int | None,
    env_vars: dict | None,
):
    """Maintain dim_date"""

    logger.info("Starting dim_date")
    # Default start date is today
    if start_date is None:
        start_date = datetime.today().date()

    # Define end date
    if num_years is None:
        end_date = start_date
    elif num_years < 0:
        end_date = start_date
        start_date = date(start_date.year + num_years, start_date.month, start_date.day)
    else:
        end_date = date(start_date.year + num_years, start_date.month, start_date.day)

    # safety check to make sure srta is earlier than end
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    logger.info("Running for range %s to %s", start_date, end_date)

    # Generate date range
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    dates_series = pd.Series(dates)

    # Create DataFrame
    date_data = pd.DataFrame(
        {
            "date": dates_series,
            "year": dates_series.dt.year,
            "month": dates_series.dt.month,
            "month_name": dates_series.dt.strftime("%B"),
            "day": dates_series.dt.day,
            "day_name": dates_series.dt.strftime("%A"),
            "week": dates_series.dt.isocalendar().week,
            "start_of_week": dates_series.dt.dayofweek == 0,  # Monday
            "end_of_week": dates_series.dt.dayofweek == 6,  # Sunday
        }
    )

    target_schema = get_variable("DB_SCHEMA", env_vars) or "public"
    folder = "lookup"
    save_to_json([date_data], ["dim_date.json"], folder, env_vars)

    maintain_table(db_engine, "dim_date", target_schema, "dimension", folder, env_vars)
