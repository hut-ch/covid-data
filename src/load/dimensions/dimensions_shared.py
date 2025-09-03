"""Manage EU specific Dimensions"""
import pandas as pd
from datetime import datetime, timedelta, date
from utils import get_logger, get_variable, save_to_json

# from .manage_dimensions import process_dimension

logger = get_logger(__name__)


def maintain_shared_dims(env_vars: dict | None):
    "Run EU loads"

    logger.info("Starting Shared Dimension Load")

    dim_date(start_year=None, num_years=10, env_vars)


def dim_date(start_date: date | None, num_years: int | None, env_vars: dict | None):
    """Maintain dim_date"""
    
    # Default start date is today
    if start_date is None:
        start_date = datetime.today().date()

    start_year = start_date.year

    # Define end date
    if num_years is None:
        end_date = start_date
    else:
        end_date = date(start_date.year - num_years, start_date.month, start_date.day)

    # Generate date range
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    # Create DataFrame
    date_data = pd.DataFrame({
        "date": dates,
        "year": dates.year,
        "month": dates.month,
        "month_name": dates.strftime('%B'),
        "day": dates.day,
        "day_name": dates.strftime('%A'),
        "week": dates.isocalendar().week,
        "start_of_week": dates.dayofweek == 0,  # Monday
        "end_of_week": dates.dayofweek == 6,    # Sunday
        "created_ts": datetime.now(),
        "updated_ts": datetime.now()
    })

    date_data

    folder = "lookup"
    save_to_json(date_data, "date.json", folder, env_vars)

    maintain_table(db_engine, "dim_date", target_schema, folder, env_vars)
