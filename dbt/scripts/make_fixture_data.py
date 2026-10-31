"""Write a tiny, deterministic raw dataset so `dbt build` runs without network or real data.

Usage (TW_DATA_DIR must point at a scratch directory, so fixtures never mix with real data):

    TW_DATA_DIR=/tmp/tw python dbt/scripts/make_fixture_data.py
    TW_DATA_DIR=/tmp/tw dbt build --project-dir dbt --profiles-dir dbt

The files follow the raw Parquet contracts in CLAUDE.md. They cover 2024-03-30 .. 2024-04-10:
two source months (so the partition glob is exercised), weekends, a half-day holiday
(2024-04-09, Ramadan Feast Eve) and a full holiday (2024-04-10), all three transport modes,
dry, rainy and snowy hours, and padded strings for the staging trim logic.
"""

from __future__ import annotations

import math
import os
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transit_weather.config import IBB_RAW_DIR, WEATHER_RAW_DIR, month_partition_path

FIRST_DAY = date(2024, 3, 30)
LAST_DAY = date(2024, 4, 10)
LOADED_AT = datetime(2024, 5, 1, 12, 0, 0)

IBB_SCHEMA = pa.schema(
    [
        ("transition_date", pa.date32()),
        ("transition_hour", pa.int8()),
        ("transport_type_id", pa.int8()),
        ("road_type", pa.string()),
        ("line", pa.string()),
        ("transfer_type", pa.string()),
        ("number_of_passage", pa.int32()),
        ("number_of_passenger", pa.int32()),
        ("product_kind", pa.string()),
        ("transaction_type_desc", pa.string()),
        ("town", pa.string()),
        ("line_name", pa.string()),
        ("station_poi_desc_cd", pa.string()),
        ("source_month", pa.string()),
        ("loaded_at", pa.timestamp("us")),
    ]
)

WEATHER_SCHEMA = pa.schema(
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

# (transport_type_id, road_type, line, line_name, town, station, base passages per hour)
LINES = [
    (1, "OTOYOL", "500T", " TUZLA - CEVIZLIBAG ", "KADIKOY ", "", 900),
    (1, "OTOYOL", "34", "METROBUS", "SISLI", "ZINCIRLIKUYU", 2500),
    (2, "RAYLI", "M2", "YENIKAPI - HACIOSMAN", "SISLI", "TAKSIM", 3000),
    (2, "RAYLI", "MARMARAY", "HALKALI - GEBZE", " USKUDAR", "USKUDAR", 2200),
    (3, "DENIZ", "KADIKOY-KARAKOY", "KADIKOY - KARAKOY", "KADIKOY", "KADIKOY ISKELE", 400),
]
FARES = [("TAM", "Tam", "Aktarmasiz"), ("INDIRIMLI", "Ogrenci", "Aktarma")]

# Rainy hours (start, number of hours) and one snowy hour, in local time.
RAIN_SPELLS = [(datetime(2024, 3, 31, 7), 5), (datetime(2024, 4, 2, 16), 4)]
RAIN_SPELLS += [(datetime(2024, 4, 6, 10), 6), (datetime(2024, 4, 8, 6), 3)]
SNOW_HOURS = {datetime(2024, 4, 3, 5)}


def hours() -> list[datetime]:
    start = datetime.combine(FIRST_DAY, datetime.min.time())
    n = ((LAST_DAY - FIRST_DAY).days + 1) * 24
    return [start + timedelta(hours=i) for i in range(n)]


def rain_mm(ts: datetime) -> float:
    for start, length in RAIN_SPELLS:
        if start <= ts < start + timedelta(hours=length):
            return 1.5
    return 0.0


def daily_profile(hour: int) -> float:
    """Commuter-shaped demand curve: morning and evening peaks, quiet nights."""
    if hour < 5:
        return 0.05
    return 0.3 + math.exp(-((hour - 8) ** 2) / 4) + 0.9 * math.exp(-((hour - 18) ** 2) / 5)


def weather_table() -> pa.Table:
    rows = []
    for ts in hours():
        precip = rain_mm(ts)
        snow = 0.3 if ts in SNOW_HOURS else 0.0
        if snow:
            precip = 0.2
        temp = 12 + 6 * math.sin((ts.hour - 9) / 24 * 2 * math.pi) - (4 if precip else 0)
        if ts.hour == 15 and ts.day % 3 == 0:
            temp = 26.0  # a few warm hours so every temp band appears
        if snow:
            temp = 0.5
        rows.append(
            {
                "observed_at": ts,
                "temperature_2m": round(temp, 1),
                "precipitation": precip,
                "rain": 0.0 if snow else precip,
                "snowfall": snow,
                "wind_speed_10m": 25.0 if precip else 9.0,
                "relative_humidity_2m": 92.0 if precip else 60.0,
                "weather_code": 73 if snow else (63 if precip else 1),
                "loaded_at": LOADED_AT,
            }
        )
    return pa.Table.from_pylist(rows, schema=WEATHER_SCHEMA)


def ibb_table(rng: random.Random) -> pa.Table:
    rows = []
    for ts in hours():
        weekend = ts.isoweekday() >= 6 or ts.date() == date(2024, 4, 10)
        weather_factor = 0.85 if rain_mm(ts) else 1.0
        for type_id, road_type, line, line_name, town, station, base in LINES:
            if type_id == 3 and not 7 <= ts.hour <= 22:
                continue  # ferries do not run at night
            for product_kind, fare, transfer in FARES:
                demand = base * daily_profile(ts.hour) * (0.6 if weekend else 1.0)
                passages = int(demand * weather_factor * rng.uniform(0.9, 1.1)) + 1
                rows.append(
                    {
                        "transition_date": ts.date(),
                        "transition_hour": ts.hour,
                        "transport_type_id": type_id,
                        "road_type": road_type,
                        "line": line,
                        "transfer_type": transfer,
                        "number_of_passage": passages,
                        "number_of_passenger": max(1, int(passages * 0.93)),
                        "product_kind": product_kind,
                        "transaction_type_desc": f" {fare} ",
                        "town": town,
                        "line_name": line_name,
                        "station_poi_desc_cd": station,
                        "source_month": ts.strftime("%Y-%m"),
                        "loaded_at": LOADED_AT,
                    }
                )
    return pa.Table.from_pylist(rows, schema=IBB_SCHEMA)


def write_by_month(table: pa.Table, month_of_row: list[str], base: Path) -> list[Path]:
    written = []
    for month in sorted(set(month_of_row)):
        mask = pa.array([m == month for m in month_of_row])
        path = month_partition_path(base, month)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table.filter(mask), path)
        written.append(path)
    return written


def main() -> int:
    if not os.getenv("TW_DATA_DIR"):
        print(
            "TW_DATA_DIR is not set. Point it at a scratch directory; fixture data must never be "
            "written next to real ingested data.",
            file=sys.stderr,
        )
        return 1

    rng = random.Random(42)
    weather = weather_table()
    ibb = ibb_table(rng)
    weather_months = [ts.strftime("%Y-%m") for ts in weather.column("observed_at").to_pylist()]
    ibb_months = ibb.column("source_month").to_pylist()

    written = write_by_month(weather, weather_months, WEATHER_RAW_DIR)
    written += write_by_month(ibb, ibb_months, IBB_RAW_DIR)
    for path in written:
        print(f"wrote {path}")
    print(f"{ibb.num_rows} İBB rows, {weather.num_rows} weather rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
