"""Dagster definitions: monthly raw ingestion assets, dbt models, asset checks and a schedule.

Asset graph::

    ibb_raw (monthly) ─────┐
                           ├─► dbt staging ─► intermediate ─► marts
    weather_raw (monthly) ─┘

The raw assets are partitioned by month (``START_MONTH``..``END_MONTH`` from
``transit_weather.config``). The dbt models are unpartitioned: each ``dbt build`` rebuilds the
warehouse from every raw partition on disk.

This module must always import, even before the ingestion modules or the dbt models exist:

* ingestion functions are imported lazily inside the assets, so a missing implementation only
  fails the run that needs it;
* the dbt assets are only added when a dbt manifest is available (``dagster dev`` parses the
  project via ``prepare_if_dev``; elsewhere run ``dbt parse`` or ``dbt build`` first).
"""

import importlib
import logging
import os
import shutil
import sys
from collections.abc import Callable, Iterator, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    AssetSpec,
    Definitions,
    Failure,
    MaterializeResult,
    MetadataValue,
    MonthlyPartitionsDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from transit_weather import config

logger = logging.getLogger(__name__)

# dbt resolves its data directory from TW_DATA_DIR, falling back to a path relative to its
# profiles dir, which dagster-dbt passes in a form that breaks that fallback. Hand the dbt
# subprocess the same absolute directory the ingestion assets write to.
os.environ.setdefault("TW_DATA_DIR", str(config.DATA_DIR.resolve()))

RAW_GROUP = "raw"
ROW_COUNT_CHECK = "partition_row_count_positive"
WEATHER_HOURS_CHECK = "every_hour_present"

# dbt source (`raw.ibb`, `raw.weather` in dbt/models/staging/sources.yml) -> Dagster asset key.
DBT_SOURCE_TO_ASSET = {"ibb": "ibb_raw", "weather": "weather_raw"}


# --------------------------------------------------------------------------------------------
# Partitions
# --------------------------------------------------------------------------------------------


def _first_day(month: str) -> date:
    year, mm = (int(part) for part in month.split("-"))
    return date(year, mm, 1)


def _next_month(day: date) -> date:
    return date(day.year + day.month // 12, day.month % 12 + 1, 1)


def build_monthly_partitions(start_month: str, end_month: str) -> MonthlyPartitionsDefinition:
    """Monthly partitions covering ``start_month``..``end_month`` inclusive ('YYYY-MM')."""
    start = _first_day(start_month)
    end_exclusive = _next_month(_first_day(end_month))
    return MonthlyPartitionsDefinition(
        start_date=start.isoformat(),
        end_date=end_exclusive.isoformat(),
        timezone=config.TIMEZONE,
    )


monthly_partitions = build_monthly_partitions(config.START_MONTH, config.END_MONTH)


def partition_month(partition_key: str) -> str:
    """'2024-03-01' (Dagster monthly partition key) -> '2024-03' (ingestion month)."""
    return partition_key[:7]


# --------------------------------------------------------------------------------------------
# Data quality helpers (pure functions, unit tested)
# --------------------------------------------------------------------------------------------


def parquet_row_count(path: Path) -> int:
    """Row count from the Parquet footer, without reading the data."""
    return pq.ParquetFile(path).metadata.num_rows


def expected_weather_hours(month: str) -> int:
    """Hours in a month in Europe/Istanbul local time.

    Istanbul has been on permanent UTC+3 since 2016, so there are no DST gaps or repeats and
    every month has ``days * 24`` local hours.
    """
    start = _first_day(month)
    return (_next_month(start) - start).days * 24


def missing_weather_hours(path: Path, month: str) -> list[datetime]:
    """Local hours of ``month`` that have no row in the weather Parquet file."""
    start = _first_day(month)
    end = _next_month(start)
    with duckdb.connect() as con:
        rows = con.execute(
            """
            SELECT hour_ts
            FROM generate_series(?::TIMESTAMP, ?::TIMESTAMP - INTERVAL 1 HOUR, INTERVAL 1 HOUR)
                AS g(hour_ts)
            WHERE hour_ts NOT IN (
                SELECT date_trunc('hour', observed_at)
                FROM read_parquet(?)
                WHERE observed_at IS NOT NULL
            )
            ORDER BY hour_ts
            """,
            [start, end, str(path)],
        ).fetchall()
    return [row[0] for row in rows]


def row_count_check(path: Path | None, *, empty_month_reason: str) -> AssetCheckResult:
    """``rows > 0``. An empty source month is reported as a warning, not an error."""
    if path is None:
        return AssetCheckResult(
            check_name=ROW_COUNT_CHECK,
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"num_rows": 0, "reason": empty_month_reason},
        )
    num_rows = parquet_row_count(path)
    return AssetCheckResult(
        check_name=ROW_COUNT_CHECK,
        passed=num_rows > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"num_rows": num_rows},
    )


def weather_hours_check(path: Path | None, month: str) -> AssetCheckResult:
    """Every local hour of the month has a weather observation."""
    expected = expected_weather_hours(month)
    if path is None:
        return AssetCheckResult(
            check_name=WEATHER_HOURS_CHECK,
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"expected_hours": expected, "missing_hours": expected},
        )
    missing = missing_weather_hours(path, month)
    return AssetCheckResult(
        check_name=WEATHER_HOURS_CHECK,
        passed=not missing,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "expected_hours": expected,
            "missing_hours": len(missing),
            "first_missing": MetadataValue.text(
                ", ".join(ts.isoformat(sep=" ") for ts in missing[:10]) or "-"
            ),
        },
    )


# --------------------------------------------------------------------------------------------
# Raw ingestion assets
# --------------------------------------------------------------------------------------------

IngestFn = Callable[[str], Path | None]


def load_ingest_fn(source: str) -> IngestFn:
    """Import ``transit_weather.ingestion.<source>.ingest_month`` at run time.

    Lazy so that this module (and the Dagster UI) loads before the ingestion code exists.
    """
    module_name = f"transit_weather.ingestion.{source}"
    try:
        module = importlib.import_module(module_name)
        return module.ingest_month
    except (ImportError, AttributeError) as exc:
        raise Failure(
            description=f"{module_name}.ingest_month is not available yet.",
            metadata={"error": repr(exc)},
        ) from exc


def _file_metadata(path: Path, num_rows: int) -> dict[str, Any]:
    return {
        "path": MetadataValue.path(str(path)),
        "num_rows": num_rows,
        "size_mb": round(path.stat().st_size / 1_000_000, 2),
        "empty_source_month": False,
    }


@asset(
    partitions_def=monthly_partitions,
    group_name=RAW_GROUP,
    kinds={"python", "parquet"},
    description=(
        "İBB hourly public transport for one source month, original grain, written to "
        "data/raw/ibb/year=YYYY/month=MM/data.parquet. Empty source months are skipped."
    ),
    check_specs=[
        AssetCheckSpec(
            ROW_COUNT_CHECK,
            asset="ibb_raw",
            description="The partition's Parquet file has at least one row.",
        )
    ],
)
def ibb_raw(context: AssetExecutionContext) -> MaterializeResult:
    month = partition_month(context.partition_key)
    path = load_ingest_fn("ibb")(month)
    if path is None:
        reason = f"İBB published an empty file (header only) for {month}; nothing written."
        context.log.warning(reason)
        return MaterializeResult(
            metadata={"source_month": month, "empty_source_month": True, "num_rows": 0},
            check_results=[row_count_check(None, empty_month_reason=reason)],
        )
    path = Path(path)
    num_rows = parquet_row_count(path)
    return MaterializeResult(
        metadata={"source_month": month, **_file_metadata(path, num_rows)},
        check_results=[row_count_check(path, empty_month_reason="")],
    )


@asset(
    partitions_def=monthly_partitions,
    group_name=RAW_GROUP,
    kinds={"python", "parquet"},
    description=(
        "Open-Meteo hourly weather for Istanbul (local time) for one month, written to "
        "data/raw/weather/year=YYYY/month=MM/data.parquet."
    ),
    check_specs=[
        AssetCheckSpec(
            ROW_COUNT_CHECK,
            asset="weather_raw",
            description="The partition's Parquet file has at least one row.",
        ),
        AssetCheckSpec(
            WEATHER_HOURS_CHECK,
            asset="weather_raw",
            description="Every local hour of the month has an observation.",
        ),
    ],
)
def weather_raw(context: AssetExecutionContext) -> MaterializeResult:
    month = partition_month(context.partition_key)
    path = load_ingest_fn("weather")(month)
    if path is None:
        reason = f"Open-Meteo returned no data for {month}; nothing written."
        context.log.warning(reason)
        return MaterializeResult(
            metadata={"source_month": month, "empty_source_month": True, "num_rows": 0},
            check_results=[
                row_count_check(None, empty_month_reason=reason),
                weather_hours_check(None, month),
            ],
        )
    path = Path(path)
    num_rows = parquet_row_count(path)
    return MaterializeResult(
        metadata={"source_month": month, **_file_metadata(path, num_rows)},
        check_results=[
            row_count_check(path, empty_month_reason=""),
            weather_hours_check(path, month),
        ],
    )


# --------------------------------------------------------------------------------------------
# dbt assets
# --------------------------------------------------------------------------------------------


def _build_dbt_assets(project_dir: Path) -> list[AssetsDefinition]:
    """dbt models as Dagster assets, or ``[]`` if the dbt project cannot be loaded yet."""
    try:
        from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
    except ImportError:  # pragma: no cover - dagster-dbt is a declared dependency
        logger.warning("dagster-dbt is not installed; dbt assets are disabled.")
        return []

    class TransitWeatherTranslator(DagsterDbtTranslator):
        """Point the dbt raw sources at the ingestion assets and group models by layer."""

        def get_asset_spec(
            self,
            manifest: Mapping[str, Any],
            unique_id: str,
            project: DbtProject | None,
        ) -> AssetSpec:
            spec = super().get_asset_spec(manifest, unique_id, project)
            props = self.get_resource_props(manifest, unique_id)
            if props["resource_type"] == "source" and props["source_name"] == "raw":
                asset_name = DBT_SOURCE_TO_ASSET.get(props["name"])
                if asset_name:
                    return spec.replace_attributes(key=AssetKey(asset_name))
            if props["resource_type"] in ("model", "seed", "snapshot"):
                fqn = props.get("fqn", [])
                layer = fqn[1] if len(fqn) > 2 else "dbt"
                return spec.replace_attributes(group_name=layer)
            return spec

    try:
        dbt_project = DbtProject(project_dir=project_dir, profiles_dir=project_dir)
        dbt_project.prepare_if_dev()
        if not dbt_project.manifest_path.exists():
            logger.warning(
                "No dbt manifest at %s; dbt assets are disabled. Run `dbt parse --project-dir "
                "dbt --profiles-dir dbt` or start `dagster dev`.",
                dbt_project.manifest_path,
            )
            return []

        @dbt_assets(
            manifest=dbt_project.manifest_path,
            project=dbt_project,
            dagster_dbt_translator=TransitWeatherTranslator(),
        )
        def transit_weather_dbt(
            context: AssetExecutionContext, dbt: DbtCliResource
        ) -> Iterator[Any]:
            # No partial parsing: a cached parse from a plain `dbt build` keeps that run's
            # source paths, which may be relative to a different working directory.
            yield from dbt.cli(["build", "--no-partial-parse"], context=context).stream()

    except Exception:  # noqa: BLE001 - a broken dbt project must not take the UI down
        logger.exception("Could not load the dbt project at %s; dbt assets disabled.", project_dir)
        return []

    return [transit_weather_dbt]


def dbt_executable() -> str:
    """The dbt installed next to this interpreter, so an inactive virtualenv still works."""
    scripts_dir = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        candidate = scripts_dir / name
        if candidate.exists():
            return str(candidate)
    return shutil.which("dbt") or "dbt"


def _dbt_resources(project_dir: Path) -> dict[str, Any]:
    try:
        from dagster_dbt import DbtCliResource
    except ImportError:  # pragma: no cover
        return {}
    return {
        "dbt": DbtCliResource(
            project_dir=str(project_dir),
            profiles_dir=str(project_dir),
            dbt_executable=dbt_executable(),
        )
    }


dbt_model_assets = _build_dbt_assets(config.DBT_PROJECT_DIR)


# --------------------------------------------------------------------------------------------
# Jobs and schedules
# --------------------------------------------------------------------------------------------

raw_selection = AssetSelection.assets(ibb_raw, weather_raw)
dbt_selection = AssetSelection.assets(*dbt_model_assets)

# Scheduled path: ingest one month, then rebuild the warehouse in the same run.
monthly_refresh_job = define_asset_job(
    name="monthly_refresh_job",
    selection=raw_selection | dbt_selection,
    description="Ingest one month of İBB + weather data, then `dbt build` the warehouse.",
)

# Backfill path: ingest many partitions with `ingest_month_job`, then run `dbt_build_job`
# once, instead of rebuilding the warehouse after every month.
ingest_month_job = define_asset_job(
    name="ingest_month_job",
    selection=raw_selection,
    description="Ingest one month of İBB ridership and Open-Meteo weather into raw Parquet.",
)
dbt_build_job = define_asset_job(
    name="dbt_build_job",
    selection=dbt_selection,
    description="Rebuild the dbt models (staging, intermediate, marts) and run dbt tests.",
)

# The 7th of each month at 03:00 Istanbul time, for the month that just ended: Open-Meteo's
# archive lags a few days behind real time.
monthly_refresh_schedule = build_schedule_from_partitioned_job(
    monthly_refresh_job,
    name="monthly_refresh_schedule",
    day_of_month=7,
    hour_of_day=3,
    minute_of_hour=0,
)

defs = Definitions(
    assets=[ibb_raw, weather_raw, *dbt_model_assets],
    jobs=[monthly_refresh_job, ingest_month_job, *([dbt_build_job] if dbt_model_assets else [])],
    schedules=[monthly_refresh_schedule],
    resources=_dbt_resources(config.DBT_PROJECT_DIR) if dbt_model_assets else {},
)
