"""SQL behind the dashboard. Pure functions over a DuckDB connection, no Streamlit.

Every query reads the dbt marts (see dbt/models/marts):

* ``fct_hourly_ridership_weather`` — hour × transport_mode
* ``agg_daily_ridership_weather`` — date × transport_mode
* ``mart_weather_effect`` — transport_mode × hour × day_type × condition

Mart tables are looked up by name in any schema, so a custom dbt ``+schema`` still works.
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

MARTS = (
    "fct_hourly_ridership_weather",
    "agg_daily_ridership_weather",
    "mart_weather_effect",
)
MODES = ("road", "rail", "sea")
DAY_TYPES = ("weekday", "weekend_or_holiday")
DRY_CONDITION = "dry"
# A day counts as wet from 1 mm of total precipitation (the usual "rain day" threshold).
WET_DAY_MM = 1.0

# Day type of a row in the hourly/daily marts, matching mart_weather_effect.day_type.
_DAY_TYPE_SQL = "CASE WHEN is_weekend OR is_holiday THEN 'weekend_or_holiday' ELSE 'weekday' END"


@dataclass(frozen=True)
class Filters:
    """Sidebar selection. Frozen (hashable) so Streamlit can cache on it."""

    modes: tuple[str, ...]
    start: date
    end: date
    day_types: tuple[str, ...] = DAY_TYPES


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Read-only connection, so the dashboard never locks dbt out of the warehouse for writes."""
    return duckdb.connect(str(path), read_only=True)


def mart_tables(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Mart name -> quoted ``schema.table`` for the marts that exist in the warehouse."""
    rows = con.execute(
        """
        SELECT table_name, table_schema
        FROM information_schema.tables
        WHERE table_name IN (SELECT unnest(?))
        ORDER BY table_schema = 'main' DESC, table_schema
        """,
        [list(MARTS)],
    ).fetchall()
    found: dict[str, str] = {}
    for table, schema in rows:
        found.setdefault(table, f'"{schema}"."{table}"')
    return found


def missing_marts(con: duckdb.DuckDBPyConnection) -> list[str]:
    tables = mart_tables(con)
    return [name for name in MARTS if name not in tables]


def _table(con: duckdb.DuckDBPyConnection, name: str) -> str:
    try:
        return mart_tables(con)[name]
    except KeyError:
        raise LookupError(f"Mart {name!r} not found in the warehouse; run `dbt build`.") from None


def _where(filters: Filters, *, with_dates: bool = True) -> tuple[str, list]:
    """WHERE clause for the hourly/daily marts (which have date and is_weekend columns)."""
    clauses = ["list_contains(?, transport_mode)", f"list_contains(?, {_DAY_TYPE_SQL})"]
    params: list = [list(filters.modes), list(filters.day_types)]
    if with_dates:
        clauses.append("date BETWEEN ? AND ?")
        params += [filters.start, filters.end]
    return " AND ".join(clauses), params


def bounds(con: duckdb.DuckDBPyConnection) -> tuple[date, date, list[str]]:
    """First date, last date and transport modes available in the daily mart."""
    table = _table(con, "agg_daily_ridership_weather")
    first, last = con.execute(f"SELECT min(date), max(date) FROM {table}").fetchone()
    modes = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT transport_mode FROM {table} ORDER BY transport_mode"
        ).fetchall()
    ]
    known = [mode for mode in MODES if mode in modes]
    return first, last, known + [mode for mode in modes if mode not in MODES]


def kpis(con: duckdb.DuckDBPyConnection, filters: Filters) -> dict[str, float | int | None]:
    """Headline numbers for the selection.

    ``wet_day_change`` compares average daily passengers on wet days with dry days of the same
    day type (weekday/weekend), weighted by the number of wet days, so a rainy weekend is not
    compared with a dry Monday.
    """
    table = _table(con, "agg_daily_ridership_weather")
    where, params = _where(filters)
    row = con.execute(
        f"""
        WITH daily AS (
            SELECT
                date,
                {_DAY_TYPE_SQL} AS day_type,
                sum(passengers) AS passengers,
                sum(passages) AS passages,
                max(total_precipitation) AS precipitation
            FROM {table}
            WHERE {where}
            GROUP BY ALL
        ),
        by_day_type AS (
            SELECT
                day_type,
                avg(passengers) FILTER (WHERE precipitation >= ?) AS wet_avg,
                avg(passengers) FILTER (WHERE precipitation < ?) AS dry_avg,
                count(*) FILTER (WHERE precipitation >= ?) AS wet_days
            FROM daily
            GROUP BY day_type
        )
        SELECT
            (SELECT sum(passengers) FROM daily) AS total_passengers,
            (SELECT sum(passages) FROM daily) AS total_passages,
            (SELECT avg(passengers) FROM daily) AS avg_daily_passengers,
            (SELECT count(*) FROM daily) AS n_days,
            (SELECT count(*) FILTER (WHERE precipitation >= ?) FROM daily) AS wet_days,
            (
                SELECT sum(wet_days * (wet_avg / dry_avg - 1)) / nullif(sum(wet_days), 0)
                FROM by_day_type
                WHERE wet_avg IS NOT NULL AND dry_avg > 0
            ) AS wet_day_change
        """,
        [*params, WET_DAY_MM, WET_DAY_MM, WET_DAY_MM, WET_DAY_MM],
    ).fetchone()
    keys = (
        "total_passengers",
        "total_passages",
        "avg_daily_passengers",
        "n_days",
        "wet_days",
        "wet_day_change",
    )
    return dict(zip(keys, row, strict=True))


def rain_effect_by_mode(con: duckdb.DuckDBPyConnection, filters: Filters) -> pd.DataFrame:
    """``index_vs_dry`` per transport mode and weather condition.

    Averaged over hours and the selected day types, weighted by ``n_hours``. The weather
    effect mart is precomputed over the whole period, so the date filter does not apply.
    """
    table = _table(con, "mart_weather_effect")
    return con.execute(
        f"""
        SELECT
            transport_mode,
            condition,
            sum(index_vs_dry * n_hours) / nullif(sum(n_hours), 0) AS index_vs_dry,
            sum(n_hours) AS n_hours
        FROM {table}
        WHERE list_contains(?, transport_mode)
          AND list_contains(?, day_type)
          AND condition <> ?
          AND index_vs_dry IS NOT NULL
        GROUP BY ALL
        ORDER BY transport_mode, condition
        """,
        [list(filters.modes), list(filters.day_types), DRY_CONDITION],
    ).df()


def hour_condition_heatmap(con: duckdb.DuckDBPyConnection, filters: Filters) -> pd.DataFrame:
    """``index_vs_dry`` per transport mode × hour of day × condition (date filter n/a)."""
    table = _table(con, "mart_weather_effect")
    return con.execute(
        f"""
        SELECT
            transport_mode,
            hour,
            condition,
            sum(index_vs_dry * n_hours) / nullif(sum(n_hours), 0) AS index_vs_dry,
            sum(avg_passages * n_hours) / nullif(sum(n_hours), 0) AS avg_passages,
            sum(n_hours) AS n_hours
        FROM {table}
        WHERE list_contains(?, transport_mode)
          AND list_contains(?, day_type)
        GROUP BY transport_mode, hour, condition
        ORDER BY transport_mode, condition <> '{DRY_CONDITION}', condition, hour
        """,
        [list(filters.modes), list(filters.day_types)],
    ).df()


def temperature_band_effect(con: duckdb.DuckDBPyConnection, filters: Filters) -> pd.DataFrame:
    """Ridership by temperature band, controlled for time of week.

    Each hour's passengers are divided by the mean for the same mode, hour of day and
    weekday/weekend, so warm afternoons are not credited with the afternoon peak.
    ``ridership_index`` = 1 means a typical hour; bands are ordered by their mean temperature.
    """
    table = _table(con, "fct_hourly_ridership_weather")
    where, params = _where(filters)
    return con.execute(
        f"""
        WITH scoped AS (
            SELECT *
            FROM {table}
            WHERE {where} AND temp_band IS NOT NULL
        ),
        indexed AS (
            SELECT
                transport_mode,
                temp_band,
                temperature_2m,
                passengers,
                passengers / nullif(
                    avg(passengers) OVER (PARTITION BY transport_mode, hour, is_weekend), 0
                ) AS ridership_index
            FROM scoped
        )
        SELECT
            transport_mode,
            temp_band,
            avg(ridership_index) AS ridership_index,
            avg(passengers) AS avg_hourly_passengers,
            avg(temperature_2m) AS avg_temperature,
            count(*) AS n_hours
        FROM indexed
        GROUP BY ALL
        ORDER BY transport_mode, avg_temperature
        """,
        params,
    ).df()


def daily_series(con: duckdb.DuckDBPyConnection, filters: Filters) -> pd.DataFrame:
    """Daily passengers per mode with that day's precipitation and mean temperature."""
    table = _table(con, "agg_daily_ridership_weather")
    where, params = _where(filters)
    return con.execute(
        f"""
        SELECT
            date,
            transport_mode,
            passengers,
            passages,
            total_precipitation,
            avg_temperature,
            rainy_hours,
            is_weekend,
            is_holiday
        FROM {table}
        WHERE {where}
        ORDER BY date, transport_mode
        """,
        params,
    ).df()
