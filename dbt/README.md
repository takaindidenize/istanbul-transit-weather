# dbt project — `transit_weather`

dbt-duckdb project that turns the raw İBB ridership and Open-Meteo weather Parquet files into the
marts the dashboard reads. All models and the seed are built into the `main` schema of
`<data dir>/warehouse.duckdb`.

## Where the data lives

`profiles.yml` (warehouse path) and `models/staging/sources.yml` (raw Parquet) resolve the data
directory the same way:

1. `TW_DATA_DIR`, if set. It may be absolute, or relative to the directory you run dbt from (the
   same rule `transit_weather.config` uses).
2. Otherwise `<directory containing dbt/profiles.yml>/../data`, i.e. `<repo>/data`. This uses
   `flags.PROFILES_DIR`, so it works from the repo root and from inside `dbt/`. It assumes you
   use the `profiles.yml` in this folder (`--profiles-dir dbt` from the root, or run from `dbt/`,
   where dbt finds it in the working directory). With a profile in `~/.dbt/`, set `TW_DATA_DIR`.

The data directory must exist before dbt runs. DuckDB creates the warehouse file but not its
parent folder.

## Run

From the repo root, after `pip install -e ".[dev]"`:

```bash
dbt build --project-dir dbt --profiles-dir dbt
```

or from inside `dbt/`:

```bash
dbt build
```

### Fixture data (CI, no network)

`scripts/make_fixture_data.py` writes a small, deterministic dataset that follows the raw contracts
in `CLAUDE.md`. It covers 2024-03-30 to 2024-04-10: two source months, weekends, a half-day and a
full holiday, all three transport modes, and dry, rainy and snowy hours. It refuses to run unless
`TW_DATA_DIR` is set, so fixtures never end up next to real data:

```bash
export TW_DATA_DIR="$(mktemp -d)"
python dbt/scripts/make_fixture_data.py
dbt build --project-dir dbt --profiles-dir dbt
```

## Lineage

```
source raw.ibb      ─► stg_ibb__hourly_transport ─► int_ridership_hourly ─┐
source raw.weather  ─► stg_weather__hourly ────────────────────────────────┤
seed turkish_public_holidays ──────────────────────────────────────────────┴─► fct_hourly_ridership_weather

fct_hourly_ridership_weather ─► mart_weather_effect
fct_hourly_ridership_weather + stg_weather__hourly + turkish_public_holidays ─► agg_daily_ridership_weather
```

| Layer | Model | Grain | Materialisation |
|---|---|---|---|
| staging | `stg_ibb__hourly_transport` | source grain (hour × line × station × fare …) | view |
| staging | `stg_weather__hourly` | hour | view |
| intermediate | `int_ridership_hourly` | hour × transport_mode | view |
| seed | `turkish_public_holidays` | date | table |
| marts | `fct_hourly_ridership_weather` | hour × transport_mode | table, contract enforced |
| marts | `agg_daily_ridership_weather` | date × transport_mode | table, contract enforced |
| marts | `mart_weather_effect` | transport_mode × hour × day_type × condition | table, contract enforced |

The mart columns are listed under "Mart contracts" in the root `CLAUDE.md`. They are enforced with
dbt model contracts in `models/marts/_marts.yml`, so a renamed column or changed type fails the
build.

## Definitions

- `transport_mode`: `transport_type_id` 1 = `road`, 2 = `rail`, 3 = `sea`. Unknown ids raise a warning
  in staging and are dropped in `int_ridership_hourly`.
- `passages` = `number_of_passage` (card taps); `passengers` = `number_of_passenger`.
- `day_of_week`: ISO, 1 = Monday … 7 = Sunday. `is_weekend`: Saturday or Sunday.
- `is_holiday`, hourly: the date is a full-day holiday, or a half-day holiday (religious feast eves,
  Republic Day Eve) from 13:00 on. Daily: full-day holidays only.
- `is_rainy`: `precipitation >= 0.1` mm. `temp_band`: `<5`, `5-15`, `15-25`, `>=25` °C, with the
  lower bound inclusive.
- `condition` in `mart_weather_effect`: `snow` if `snowfall > 0`, else `rain` if `is_rainy`, else
  `dry`. Hours without weather are excluded.
- `index_vs_dry`: `avg_passages` divided by the dry `avg_passages` for the same mode, hour and
  day_type. It is 1.0 for dry rows and null when there is no dry baseline.

## Tests

- Generic: `not_null` and uniqueness on every grain (`tests/generic/unique_combination_of_columns.sql`
  is a dependency-free stand-in for the dbt_utils test), plus `accepted_values` on
  `transport_mode`, `day_type`, `condition`, `temp_band` and `day_of_week`.
- `assert_weather_has_no_gaps`: every hour from 00:00 on the first ridership date to 23:00 on the
  last has a weather row.
- `assert_ridership_totals_reconcile`: passages per mode are equal in staging, the hourly mart
  and the daily mart.
- `assert_dry_index_is_one`: dry rows are their own baseline.

Tests use the `arguments:` syntax, which needs dbt-core 1.10 or later.
