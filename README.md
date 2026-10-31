# Istanbul Transit × Weather

**Question:** How does weather change the way Istanbul uses public transport?

An end-to-end, laptop-scale data pipeline that joins Istanbul Metropolitan Municipality's
hourly public-transport ridership (≈1 GB of CSV per month) with hourly historical weather,
models it with dbt, orchestrates it with Dagster and serves the answers in a Streamlit dashboard.

> 🚧 Work in progress — see [docs/ROADMAP.md](docs/ROADMAP.md).

## Architecture

```
İBB Open Data (CKAN API, monthly CSV)  ─┐
                                        ├─► raw Parquet (data/raw) ─► DuckDB + dbt ─► marts ─► Streamlit
Open-Meteo Archive API (hourly JSON)  ──┘        ▲                       (staging → intermediate → marts)
                                                 └──────────── Dagster assets, schedules, checks
```

| Layer | Tool | Notes |
|---|---|---|
| Ingestion | Python, DuckDB | Monthly İBB files are streamed and written as typed Parquet; schema drift between months is normalised |
| Storage | DuckDB, Parquet | Single-file warehouse at `data/warehouse.duckdb` |
| Transformation | dbt-duckdb | Staging, intermediate and mart models with tests |
| Orchestration | Dagster | Monthly partitions, asset checks |
| Serving | Streamlit | Weather vs. ridership analysis |
| CI | GitHub Actions | Ruff, pytest, `dbt build` on fixture data |

## Data sources

- [İBB Hourly Public Transport Data Set](https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set): passages and passengers per hour, line, station, district, fare type (2020–2024)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api): hourly temperature, precipitation, snowfall, wind, humidity for Istanbul

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
dagster dev            # open http://localhost:3000 and materialise the assets
streamlit run dashboard/app.py
```

## Project layout

```
src/transit_weather/   Python package: config, ingestion, Dagster definitions
dbt/                   dbt project (DuckDB adapter)
dashboard/             Streamlit app
tests/                 pytest suite
docs/                  roadmap and data notes
```
