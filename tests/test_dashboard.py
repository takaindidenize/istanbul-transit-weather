"""Dashboard SQL (dashboard/queries.py) against a tiny synthetic warehouse."""

import importlib.util
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from transit_weather import config

QUERIES_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "queries.py"
_spec = importlib.util.spec_from_file_location("dashboard_queries", QUERIES_PATH)
queries = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(queries)

DAYS = [date(2024, 1, 1) + timedelta(days=i) for i in range(14)]  # Mon 1 .. Sun 14 Jan
WET_DAYS = {date(2024, 1, 3), date(2024, 1, 6)}  # a Wednesday and a Saturday
MODES = ("road", "rail", "sea")
BASE = {"road": 1000, "rail": 600, "sea": 50}


def _passengers(mode: str, day: date, hour: int) -> int:
    weekend = day.weekday() >= 5
    rush = 2 if hour in (8, 18) else 1
    value = BASE[mode] * rush * (0.6 if weekend else 1.0)
    if day in WET_DAYS:
        value *= 0.9
    return int(value)


@pytest.fixture
def con(tmp_path: Path):
    path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(path)) as w:
        w.execute(
            """
            CREATE TABLE fct_hourly_ridership_weather (
                observed_at TIMESTAMP, date DATE, hour INTEGER, day_of_week INTEGER,
                is_weekend BOOLEAN, is_holiday BOOLEAN, transport_mode VARCHAR,
                passages BIGINT, passengers BIGINT, temperature_2m DOUBLE,
                precipitation DOUBLE, snowfall DOUBLE, wind_speed_10m DOUBLE,
                is_rainy BOOLEAN, temp_band VARCHAR)
            """
        )
        hourly = []
        for day in DAYS:
            for hour in range(24):
                temp = 2.0 + day.day * 0.8
                rainy = day in WET_DAYS and 6 <= hour < 12
                for mode in MODES:
                    p = _passengers(mode, day, hour)
                    hourly.append(
                        (
                            datetime(day.year, day.month, day.day, hour),
                            day,
                            hour,
                            day.isoweekday(),
                            day.weekday() >= 5,
                            False,
                            mode,
                            int(p * 1.2),
                            p,
                            temp,
                            1.5 if rainy else 0.0,
                            0.0,
                            10.0,
                            rainy,
                            "<5°C" if temp < 5 else ("5-10°C" if temp < 10 else ">=10°C"),
                        )
                    )
        rows = pd.DataFrame(hourly)  # noqa: F841 - read by DuckDB's replacement scan below
        w.execute("INSERT INTO fct_hourly_ridership_weather SELECT * FROM rows")
        w.execute(
            """
            CREATE TABLE agg_daily_ridership_weather AS
            SELECT date, any_value(day_of_week) AS day_of_week, any_value(is_weekend) AS is_weekend,
                   any_value(is_holiday) AS is_holiday, transport_mode,
                   sum(passages) AS passages, sum(passengers) AS passengers,
                   avg(temperature_2m) AS avg_temperature,
                   sum(precipitation) AS total_precipitation,
                   count(*) FILTER (WHERE is_rainy) AS rainy_hours
            FROM fct_hourly_ridership_weather
            GROUP BY date, transport_mode
            """
        )
        w.execute(
            """
            CREATE TABLE mart_weather_effect AS
            WITH base AS (
                SELECT transport_mode, hour,
                       CASE WHEN is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type,
                       CASE WHEN is_rainy THEN 'rain' ELSE 'dry' END AS condition,
                       avg(passages) AS avg_passages, count(*) AS n_hours
                FROM fct_hourly_ridership_weather GROUP BY ALL
            )
            SELECT b.*, b.avg_passages / d.avg_passages AS index_vs_dry
            FROM base b JOIN base d USING (transport_mode, hour, day_type)
            WHERE d.condition = 'dry'
            """
        )
    connection = queries.connect(path)
    yield connection
    connection.close()


@pytest.fixture
def filters():
    return queries.Filters(MODES, DAYS[0], DAYS[-1])


def test_connection_is_read_only(con):
    with pytest.raises(duckdb.Error):
        con.execute("CREATE TABLE nope (x INTEGER)")


def test_mart_lookup(con, tmp_path):
    assert queries.missing_marts(con) == []
    assert queries.mart_tables(con)["mart_weather_effect"] == '"main"."mart_weather_effect"'
    with duckdb.connect(str(tmp_path / "empty.duckdb")) as empty:
        assert queries.missing_marts(empty) == list(queries.MARTS)


def test_bounds(con):
    first, last, modes = queries.bounds(con)
    assert (first, last) == (DAYS[0], DAYS[-1])
    assert modes == ["road", "rail", "sea"]


def test_kpis(con, filters):
    k = queries.kpis(con, filters)
    expected_total = sum(_passengers(m, d, h) for m in MODES for d in DAYS for h in range(24))
    assert k["total_passengers"] == expected_total
    assert k["n_days"] == 14
    assert k["wet_days"] == 2
    # Wet days carry exactly 10% fewer passengers than dry days of the same day type.
    assert k["wet_day_change"] == pytest.approx(-0.1, abs=1e-3)


def test_kpis_respect_filters(con):
    weekdays = queries.Filters(("sea",), DAYS[0], DAYS[4], ("weekday",))
    k = queries.kpis(con, weekdays)
    assert k["n_days"] == 5
    assert k["wet_days"] == 1
    assert k["total_passengers"] == sum(
        _passengers("sea", d, h) for d in DAYS[:5] for h in range(24)
    )


def test_rain_effect_by_mode(con, filters):
    df = queries.rain_effect_by_mode(con, filters)
    assert set(df["transport_mode"]) == set(MODES)
    assert set(df["condition"]) == {"rain"}  # dry is the reference, not a bar
    assert df["index_vs_dry"].between(0.85, 0.95).all()


def test_hour_condition_heatmap(con, filters):
    df = queries.hour_condition_heatmap(con, queries.Filters(("rail",), filters.start, filters.end))
    assert set(df["transport_mode"]) == {"rail"}
    assert df["condition"].iloc[0] == "dry"  # dry rows sort first
    dry = df[df["condition"] == "dry"]
    assert len(dry) == 24 and dry["index_vs_dry"].eq(1).all()
    assert set(df.loc[df["condition"] == "rain", "hour"]) == set(range(6, 12))


def test_temperature_band_effect(con, filters):
    df = queries.temperature_band_effect(con, filters)
    road = df[df["transport_mode"] == "road"]
    assert list(road["temp_band"]) == ["<5°C", "5-10°C", ">=10°C"]  # ordered by temperature
    assert road["n_hours"].sum() == 14 * 24
    weighted = (road["ridership_index"] * road["n_hours"]).sum() / road["n_hours"].sum()
    assert weighted == pytest.approx(1.0)


def test_daily_series(con):
    df = queries.daily_series(con, queries.Filters(("road", "sea"), DAYS[0], DAYS[1]))
    assert len(df) == 4
    assert set(df["transport_mode"]) == {"road", "sea"}
    assert list(df.columns[:3]) == ["date", "transport_mode", "passengers"]


def test_missing_mart_raises(tmp_path, filters):
    with duckdb.connect(str(tmp_path / "empty.duckdb")) as empty:
        with pytest.raises(LookupError, match="dbt build"):
            queries.kpis(empty, filters)


@pytest.mark.skipif(
    not config.WAREHOUSE_PATH.exists() and not os.getenv("TW_REQUIRE_BUILT"),
    reason="no warehouse built (run dbt build first)",
)
def test_queries_against_built_warehouse():
    """Contract check between the dbt marts and the dashboard, run in CI after `dbt build`."""
    with queries.connect(config.WAREHOUSE_PATH) as warehouse:
        assert queries.missing_marts(warehouse) == []
        first, last, modes = queries.bounds(warehouse)
        assert first is not None and modes
        f = queries.Filters(tuple(modes), first, last)
        queries.kpis(warehouse, f)
        for name in (
            "rain_effect_by_mode",
            "hour_condition_heatmap",
            "temperature_band_effect",
            "daily_series",
        ):
            getattr(queries, name)(warehouse, f)


# --- Streamlit app smoke tests ------------------------------------------------------------

APP_PATH = str(QUERIES_PATH.with_name("app.py"))


def test_app_explains_missing_warehouse(monkeypatch, tmp_path):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setattr(config, "WAREHOUSE_PATH", tmp_path / "nope.duckdb")
    at = AppTest.from_file(APP_PATH, default_timeout=60).run()
    assert not at.exception
    assert any("No warehouse found" in w.value for w in at.warning)


def test_app_renders_all_sections(monkeypatch, con, tmp_path):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setattr(config, "WAREHOUSE_PATH", tmp_path / "warehouse.duckdb")
    at = AppTest.from_file(APP_PATH, default_timeout=60).run()
    assert not at.exception
    assert [m.label for m in at.metric][:2] == ["Passengers", "Avg passengers / day"]
    assert len(at.subheader) == 4
    at.sidebar.radio[0].set_value("Weekends").run()
    assert not at.exception
