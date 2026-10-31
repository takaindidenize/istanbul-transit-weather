# Roadmap

## Phase 1 — parallel build (one branch / PR each)
- [ ] **İBB ingestion** — list monthly resources from the CKAN API
      (`package_show?id=hourly-public-transport-data-set`), download/stream each CSV, normalise schema
      drift, write the raw Parquet contract, skip empty months, CLI entry point.
- [ ] **Weather ingestion** — fetch hourly history for Istanbul from the Open-Meteo archive API
      per month, write the raw Parquet contract, retry/backoff, CLI entry point.
- [ ] **dbt models** — staging, intermediate and marts (hourly ridership × weather, daily summary,
      rain/temperature effect by transport mode), schema + data tests, fixture seeds so
      `dbt build` runs in CI without real data.
- [ ] **Orchestration, dashboard, CI** — Dagster assets with monthly partitions wrapping ingestion
      and dbt, asset checks, Streamlit dashboard over the marts, GitHub Actions workflow.

## Phase 2 — integration
- [ ] Run end to end on 12 months of real data, fix integration issues.
- [ ] Analysis write-up in the README with charts and findings.
- [ ] Data quality notes (`docs/DATA_NOTES.md`): schema drift, empty months, outliers.
