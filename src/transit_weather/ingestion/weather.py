"""Ingest Open-Meteo hourly weather for Istanbul into raw Parquet, one partition per month.

Usage: python -m transit_weather.ingestion.weather --start 2024-01 --end 2024-12
"""

import argparse
import calendar
import logging
from datetime import date, datetime
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from transit_weather import config
from transit_weather.ingestion.months import month_range

log = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("observed_at", pa.timestamp("us")),
        ("temperature_2m", pa.float64()),
        ("precipitation", pa.float64()),
        ("rain", pa.float64()),
        ("snowfall", pa.float64()),
        ("wind_speed_10m", pa.float64()),
        ("relative_humidity_2m", pa.float64()),
        ("weather_code", pa.int16()),
        ("loaded_at", pa.timestamp("us")),
    ]
)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, max=30),
    reraise=True,
)
def fetch_month(month: str, client: httpx.Client | None = None) -> dict:
    """Raw Open-Meteo archive JSON for every hour of a 'YYYY-MM' month."""
    year, mm = (int(part) for part in month.split("-"))
    params = {
        "latitude": config.ISTANBUL_LAT,
        "longitude": config.ISTANBUL_LON,
        "start_date": date(year, mm, 1).isoformat(),
        "end_date": date(year, mm, calendar.monthrange(year, mm)[1]).isoformat(),
        "hourly": ",".join(config.WEATHER_VARIABLES),
        "timezone": config.TIMEZONE,
    }
    owns_client = client is None
    client = client or httpx.Client(timeout=60)
    try:
        response = client.get(config.OPEN_METEO_ARCHIVE_URL, params=params)
        response.raise_for_status()
        return response.json()
    finally:
        if owns_client:
            client.close()


def to_table(payload: dict, month: str) -> pa.Table:
    """Convert an Open-Meteo payload to the raw weather contract, logging quality issues."""
    hourly = payload["hourly"]
    times = [datetime.fromisoformat(t) for t in hourly["time"]]

    year, mm = (int(part) for part in month.split("-"))
    expected = calendar.monthrange(year, mm)[1] * 24  # Turkey has had no DST since 2016
    if len(times) != expected:
        log.warning("weather %s: %d hours, expected %d", month, len(times), expected)
    if len(set(times)) != len(times):
        log.warning("weather %s: duplicate timestamps", month)
    for name in config.WEATHER_VARIABLES:
        nulls = sum(v is None for v in hourly.get(name, []))
        if nulls:
            log.warning("weather %s: %d null values in %s", month, nulls, name)

    loaded_at = datetime.now().replace(microsecond=0)
    columns = {"observed_at": times}
    for name in config.WEATHER_VARIABLES:
        columns[name] = hourly.get(name, [None] * len(times))
    columns["loaded_at"] = [loaded_at] * len(times)
    return pa.Table.from_pydict(columns, schema=SCHEMA)


def ingest_month(month: str, client: httpx.Client | None = None) -> Path | None:
    """Fetch one month and (over)write its raw Parquet partition."""
    table = to_table(fetch_month(month, client), month)
    if table.num_rows == 0:
        log.warning("weather %s: no rows returned, skipping", month)
        return None
    path = config.month_partition_path(config.WEATHER_RAW_DIR, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    log.info("weather %s: wrote %d rows to %s", month, table.num_rows, path)
    return path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=config.START_MONTH)
    parser.add_argument("--end", default=config.END_MONTH)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with httpx.Client(timeout=60) as client:
        for month in month_range(args.start, args.end):
            ingest_month(month, client)


if __name__ == "__main__":
    main()
