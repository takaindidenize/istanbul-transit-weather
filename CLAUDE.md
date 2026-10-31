# Project guide for Claude

Portfolio project: İBB hourly public transport × Open-Meteo weather pipeline.
Read `README.md` and `docs/ROADMAP.md` first. Code, comments, commits and docs are in English.

## Stack
Python 3.11+, DuckDB, dbt-duckdb, Dagster, Streamlit, pytest, ruff. All dependencies are already
declared in `pyproject.toml`; add one only if truly needed and say why in the PR.

## Layout and ownership
Several sessions work on this repo in parallel, each on its own branch. Stay inside your area:

| Area | Files |
|---|---|
| İBB ingestion | `src/transit_weather/ingestion/ibb.py`, `tests/test_ibb.py` |
| Weather ingestion | `src/transit_weather/ingestion/weather.py`, `tests/test_weather.py` |
| dbt models | `dbt/**` |
| Orchestration, dashboard, CI | `src/transit_weather/definitions.py`, `dashboard/**`, `.github/**`, `tests/test_definitions.py` |

`src/transit_weather/config.py` and the raw contracts below are shared: do not change them without
updating every consumer in the same PR.

## Raw data contracts
Ingestion writes Parquet; dbt reads it through `dbt/models/staging/sources.yml`.

`data/raw/ibb/year=YYYY/month=MM/data.parquet` — one file per source month, original grain, columns:
`transition_date DATE, transition_hour TINYINT, transport_type_id TINYINT, road_type VARCHAR,
line VARCHAR, transfer_type VARCHAR, number_of_passage INTEGER, number_of_passenger INTEGER,
product_kind VARCHAR, transaction_type_desc VARCHAR, town VARCHAR, line_name VARCHAR,
station_poi_desc_cd VARCHAR, source_month VARCHAR ('YYYY-MM'), loaded_at TIMESTAMP`.
Source column names can drift between months; normalise them to the names above. Empty source
months (header only) are logged and skipped, not written.

`data/raw/weather/year=YYYY/month=MM/data.parquet` — hourly, Europe/Istanbul local time, columns:
`observed_at TIMESTAMP, temperature_2m DOUBLE, precipitation DOUBLE, rain DOUBLE, snowfall DOUBLE,
wind_speed_10m DOUBLE, relative_humidity_2m DOUBLE, weather_code SMALLINT, loaded_at TIMESTAMP`.

## Conventions
- Paths and month ranges come from `transit_weather.config`, never hard-coded.
- Ingestion is idempotent: re-running a month overwrites its partition.
- Tests must not hit the network; use small fixtures under `tests/fixtures/`.
- Run `ruff check . && ruff format --check . && pytest` before committing.

## Mart contracts
Built by dbt into the `main` schema of `$TW_DATA_DIR/warehouse.duckdb` (default `data/warehouse.duckdb`).
Names and types are enforced by dbt model contracts in `dbt/models/marts/_marts.yml`; changing them
is a breaking change for `dashboard/`. Definitions are in `dbt/README.md`.

`fct_hourly_ridership_weather` — grain hour × transport_mode:
`observed_at TIMESTAMP, date DATE, hour INTEGER, day_of_week INTEGER (ISO 1=Mon..7=Sun),
is_weekend BOOLEAN, is_holiday BOOLEAN (half-day holidays from 13:00), transport_mode VARCHAR
('road','rail','sea'), passages BIGINT, passengers BIGINT, temperature_2m DOUBLE, precipitation DOUBLE,
snowfall DOUBLE, wind_speed_10m DOUBLE, is_rainy BOOLEAN (precipitation >= 0.1),
temp_band VARCHAR ('<5','5-15','15-25','>=25')`.

`agg_daily_ridership_weather` — grain date × transport_mode:
`date DATE, day_of_week INTEGER, is_weekend BOOLEAN, is_holiday BOOLEAN (full-day holidays only),
transport_mode VARCHAR, passages BIGINT, passengers BIGINT, avg_temperature DOUBLE,
total_precipitation DOUBLE, rainy_hours INTEGER`. Weather columns cover all 24 hours of the day.

`mart_weather_effect` — grain transport_mode × hour × day_type × condition:
`transport_mode VARCHAR, hour INTEGER, day_type VARCHAR ('weekday','weekend_or_holiday'),
condition VARCHAR ('dry','rain','snow'; snow if snowfall > 0, else rain if is_rainy), avg_passages DOUBLE,
n_hours INTEGER, index_vs_dry DOUBLE` (avg_passages / dry avg_passages for the same mode, hour and
day_type; null without a dry baseline).
