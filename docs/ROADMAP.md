# Roadmap

## Phase 1 — build ✅
- [x] **İBB ingestion**: CKAN resource listing, streamed download, DuckDB → Parquet, schema
      normalisation, empty-month and month-name handling.
- [x] **Weather ingestion**: Open-Meteo archive per month, retries, completeness checks.
- [x] **dbt models**: staging, intermediate and marts, holiday seed, contracts, data tests,
      fixture generator for CI.
- [x] **Orchestration, dashboard, CI**: Dagster partitions, asset checks and schedule; Streamlit
      dashboard; GitHub Actions.

## Phase 2 — integration ✅
- [x] End-to-end run on real data: 2023-01 to 2024-07 (310M rows); dbt build and Dagster runs pass.
- [x] Fixed-effects analysis with clustered standard errors (`analysis/weather_effect.py`) and
      findings in the README.
- [x] Data quality notes (`docs/DATA_NOTES.md`): empty and incomplete months, mislabelled file.

## Ideas for later
- [ ] Line- or district-level effects (which lines lose the most riders in rain?), using the
      station and town columns already in the raw data.
- [ ] Several weather points (European and Asian side) instead of one city-centre grid point.
- [ ] Forecast next-day ridership from the weather forecast, evaluated against a seasonal baseline.
- [ ] Extend to 2020–2022 to compare pandemic-era behaviour.
