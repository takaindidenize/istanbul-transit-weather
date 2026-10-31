"""Dagster definitions load and the raw assets/checks behave, without network or dbt."""

import os
import sys
import types
from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from dagster import AssetCheckSeverity, AssetKey, materialize

from transit_weather import config
from transit_weather import definitions as d


def _write_weather(path: Path, month: str, drop: int = 0) -> Path:
    """Hourly weather Parquet for ``month`` with the last ``drop`` hours missing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        f"""
        COPY (
            SELECT ts AS observed_at, 10.0 AS temperature_2m
            FROM generate_series(
                TIMESTAMP '{month}-01',
                TIMESTAMP '{month}-01' + INTERVAL 1 MONTH - INTERVAL {1 + drop} HOUR,
                INTERVAL 1 HOUR
            ) AS g(ts)
        ) TO '{path}' (FORMAT parquet)
        """
    )
    return path


def _write_rows(path: Path, n: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY (SELECT range AS x FROM range({n})) TO '{path}' (FORMAT parquet)")
    return path


@pytest.fixture
def fake_ingestion(monkeypatch, tmp_path):
    """Install fake ``transit_weather.ingestion.{ibb,weather}`` modules.

    ``returns[source]`` decides what ``ingest_month`` does: 'ok', 'empty' (None) or 'gap'.
    """
    returns = {"ibb": "ok", "weather": "ok"}
    calls: list[tuple[str, str]] = []

    def make(source: str):
        def ingest_month(month: str):
            calls.append((source, month))
            mode = returns[source]
            if mode == "empty":
                return None
            path = config.month_partition_path(tmp_path / source, month)
            if source == "weather":
                return _write_weather(path, month, drop=3 if mode == "gap" else 0)
            return _write_rows(path, 5)

        module = types.ModuleType(f"transit_weather.ingestion.{source}")
        module.ingest_month = ingest_month
        monkeypatch.setitem(sys.modules, module.__name__, module)

    make("ibb")
    make("weather")
    return returns, calls


def _checks(result):
    return {
        (e.asset_key.to_user_string(), e.check_name): e
        for e in result.get_asset_check_evaluations()
    }


# --- Definitions load ---------------------------------------------------------------------


def test_definitions_load():
    defs = d.defs
    graph = defs.resolve_asset_graph()
    keys = {key.to_user_string() for key in graph.get_all_asset_keys()}
    assert {"ibb_raw", "weather_raw"} <= keys
    assert defs.resolve_job_def("monthly_refresh_job")
    assert defs.resolve_job_def("ingest_month_job")
    assert defs.resolve_schedule_def("monthly_refresh_schedule")


def test_asset_checks_registered():
    graph = d.defs.resolve_asset_graph()
    check_keys = {(k.asset_key.to_user_string(), k.name) for k in graph.asset_check_keys}
    assert ("ibb_raw", d.ROW_COUNT_CHECK) in check_keys
    assert ("weather_raw", d.ROW_COUNT_CHECK) in check_keys
    assert ("weather_raw", d.WEATHER_HOURS_CHECK) in check_keys


def test_partitions_cover_config_range():
    keys = d.monthly_partitions.get_partition_keys()
    assert d.partition_month(keys[0]) == config.START_MONTH
    assert d.partition_month(keys[-1]) == config.END_MONTH


def test_build_monthly_partitions_inclusive_and_across_years():
    parts = d.build_monthly_partitions("2023-11", "2024-02")
    assert parts.get_partition_keys() == ["2023-11-01", "2023-12-01", "2024-01-01", "2024-02-01"]


def test_schedule_runs_monthly():
    schedule = d.defs.resolve_schedule_def("monthly_refresh_schedule")
    assert schedule.cron_schedule == "0 3 7 * *"


def test_dbt_assets_depend_on_raw_assets():
    if not d.dbt_model_assets:
        if os.getenv("TW_REQUIRE_BUILT"):  # set by the CI dbt job, where a manifest must exist
            pytest.fail("dbt assets did not load although a dbt build ran")
        pytest.skip("no dbt manifest yet (run `dbt parse` or `dbt build` to enable dbt assets)")
    deps = set().union(*(a.dependency_keys for a in d.dbt_model_assets))
    assert {AssetKey("ibb_raw"), AssetKey("weather_raw")} <= deps


def test_dbt_assets_disabled_without_project(tmp_path):
    assert d._build_dbt_assets(tmp_path / "does-not-exist") == []
    (tmp_path / "dbt").mkdir()
    assert d._build_dbt_assets(tmp_path / "dbt") == []  # no manifest


def test_missing_ingestion_module_is_a_clear_failure(monkeypatch):
    module = types.ModuleType("transit_weather.ingestion.ibb")  # no ingest_month yet
    monkeypatch.setitem(sys.modules, module.__name__, module)
    with pytest.raises(Exception, match="ingest_month is not available"):
        d.load_ingest_fn("ibb")


# --- Pure check helpers -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("month", "hours"), [("2024-02", 29 * 24), ("2023-02", 28 * 24), ("2024-03", 31 * 24)]
)
def test_expected_weather_hours(month, hours):
    assert d.expected_weather_hours(month) == hours


def test_missing_weather_hours(tmp_path):
    complete = _write_weather(tmp_path / "a.parquet", "2024-02")
    assert d.missing_weather_hours(complete, "2024-02") == []
    gappy = _write_weather(tmp_path / "b.parquet", "2024-02", drop=2)
    assert d.missing_weather_hours(gappy, "2024-02") == [
        datetime(2024, 2, 29, 22),
        datetime(2024, 2, 29, 23),
    ]


def test_row_count_check(tmp_path):
    rows = _write_rows(tmp_path / "x.parquet", 3)
    no_rows = _write_rows(tmp_path / "y.parquet", 0)
    assert d.row_count_check(rows, empty_month_reason="").passed
    assert not d.row_count_check(no_rows, empty_month_reason="").passed
    empty = d.row_count_check(None, empty_month_reason="header only")
    assert not empty.passed and empty.severity == AssetCheckSeverity.WARN


# --- Asset execution (fake ingestion) -----------------------------------------------------


def test_raw_assets_materialise_with_checks(fake_ingestion):
    _, calls = fake_ingestion
    result = materialize([d.ibb_raw, d.weather_raw], partition_key="2024-02-01")
    assert result.success
    assert sorted(calls) == [("ibb", "2024-02"), ("weather", "2024-02")]
    checks = _checks(result)
    assert all(e.passed for e in checks.values()) and len(checks) == 3
    meta = result.asset_materializations_for_node("ibb_raw")[0].metadata
    assert meta["num_rows"].value == 5
    assert meta["empty_source_month"].value is False


def test_empty_source_month_is_not_a_failure(fake_ingestion):
    returns, _ = fake_ingestion
    returns["ibb"] = "empty"
    result = materialize([d.ibb_raw], partition_key="2024-03-01")
    assert result.success
    meta = result.asset_materializations_for_node("ibb_raw")[0].metadata
    assert meta["empty_source_month"].value is True
    check = _checks(result)[("ibb_raw", d.ROW_COUNT_CHECK)]
    assert not check.passed and check.severity == AssetCheckSeverity.WARN


def test_weather_gap_fails_hours_check(fake_ingestion):
    returns, _ = fake_ingestion
    returns["weather"] = "gap"
    result = materialize([d.weather_raw], partition_key="2024-04-01", raise_on_error=False)
    check = _checks(result)[("weather_raw", d.WEATHER_HOURS_CHECK)]
    assert not check.passed
    assert check.metadata["missing_hours"].value == 3
