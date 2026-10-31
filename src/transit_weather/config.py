"""Shared paths and settings. Override with environment variables."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("TW_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
IBB_RAW_DIR = RAW_DIR / "ibb"
WEATHER_RAW_DIR = RAW_DIR / "weather"
WAREHOUSE_PATH = DATA_DIR / "warehouse.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"

# Inclusive month range to ingest, 'YYYY-MM'. Each İBB month is ~1 GB of CSV. The default is the
# last complete stretch: İBB published Aug-Oct 2024 only partially and Nov-Dec 2024 empty
# (see docs/DATA_NOTES.md).
START_MONTH = os.getenv("TW_START_MONTH", "2023-01")
END_MONTH = os.getenv("TW_END_MONTH", "2024-07")

IBB_CKAN_URL = "https://data.ibb.gov.tr/api/3/action/package_show"
IBB_DATASET_ID = "hourly-public-transport-data-set"

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
ISTANBUL_LAT = 41.01
ISTANBUL_LON = 28.98
TIMEZONE = "Europe/Istanbul"
WEATHER_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "relative_humidity_2m",
    "weather_code",
]


def month_partition_path(base: Path, month: str) -> Path:
    """Parquet path for a 'YYYY-MM' month under a raw base directory."""
    year, mm = month.split("-")
    return base / f"year={year}" / f"month={mm}" / "data.parquet"
