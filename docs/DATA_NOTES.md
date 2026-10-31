# Data notes

Findings about the sources and how the pipeline handles them. Verified on 2026-10-31.

## İBB Hourly Public Transport Data Set

Source: [data.ibb.gov.tr](https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set),
listed through the CKAN API (`package_show?id=hourly-public-transport-data-set`).

| Finding | Handling |
|---|---|
| 60 monthly CSV resources, January 2020 – December 2024. Each is roughly 1 GB and 16–17 million rows (January 2024: 16,727,406 rows, 86 MB as zstd Parquet). | Streamed to a temporary file and converted with DuckDB; never loaded into pandas. One month takes about 35 seconds on a home connection. |
| **November and December 2024 are published as header-only files** (no data rows). | `ingest_month` returns `None`, writes nothing and logs a warning; the Dagster asset reports a WARN-severity check instead of failing. |
| **The December 2024 resource points to `hourly_transportation_202512.csv`** – the file name says December 2025. | The month is parsed from the Turkish resource name (`Aralık 2024 …`), cross-checked against the file name, and the mismatch is logged. |
| **August–October 2024 are incomplete.** Complete months carry 6–7.7 million passages per day; these carry 1.1, 1.5 and 2.2 million, and October covers only 18 days (see table below). | Excluded: the default ingestion range is January 2023 – July 2024, the last complete stretch (`TW_START_MONTH` / `TW_END_MONTH` override it). |
| The data dictionary on the portal describes some months with different field names (for example `number_of_passage` vs `number_of_passenger`), but **all 60 CSV headers are actually identical**. | Column names are still normalised (case, spaces, known aliases) and a missing column becomes NULL with a warning, so a future drift does not break ingestion. |
| `transition_hour` is zero-padded text (`00`–`23`); numbers arrive as text. | Read as text and `TRY_CAST` to the contract types; rows that fail to parse the date, hour or transport type are counted and logged. |
| `transport_type_id`: 1 = road (bus, metrobus), 2 = rail (metro, tram, Marmaray, funicular), 3 = sea (ferries). | Mapped to `transport_mode` in `stg_ibb__hourly_transport`. |
| Per the portal's data dictionary, `number_of_passage` counts journeys (card taps, transfers included) and `number_of_passenger` counts individual passengers in that hour and cell. | The analysis uses passages; both are kept in the marts. |

### Monthly profile, 2023-01 to 2024-10

| Month | Rows | Days | Passages (M) | Passages / day (M) |
|---|---:|---:|---:|---:|
| 2023-01 | 15,094,696 | 31 | 211.6 | 6.83 |
| 2023-02 | 13,325,989 | 28 | 169.9 | 6.07 |
| 2023-03 | 15,409,759 | 31 | 206.0 | 6.64 |
| 2023-04 | 15,235,670 | 30 | 195.5 | 6.52 |
| 2023-05 | 16,530,169 | 31 | 214.8 | 6.93 |
| 2023-06 | 15,707,148 | 30 | 195.5 | 6.52 |
| 2023-07 | 15,997,124 | 31 | 187.4 | 6.05 |
| 2023-08 | 16,254,271 | 31 | 189.6 | 6.12 |
| 2023-09 | 16,439,146 | 30 | 200.8 | 6.69 |
| 2023-10 | 17,227,770 | 31 | 236.7 | 7.64 |
| 2023-11 | 16,107,029 | 30 | 217.0 | 7.23 |
| 2023-12 | 16,693,084 | 31 | 225.8 | 7.28 |
| 2024-01 | 16,727,406 | 31 | 208.4 | 6.72 |
| 2024-02 | 16,360,672 | 29 | 212.9 | 7.34 |
| 2024-03 | 17,172,222 | 31 | 223.5 | 7.21 |
| 2024-04 | 16,785,692 | 30 | 212.8 | 7.09 |
| 2024-05 | 18,338,197 | 31 | 238.0 | 7.68 |
| 2024-06 | 16,787,260 | 30 | 190.5 | 6.35 |
| 2024-07 | 17,711,578 | 31 | 193.9 | 6.26 |
| **2024-08** | 2,954,106 | 31 | 33.0 | **1.06** |
| **2024-09** | 3,958,389 | 30 | 46.2 | **1.54** |
| **2024-10** | 2,770,719 | **18** | 39.1 | **2.17** |

## Open-Meteo Historical Weather API

Source: [archive-api.open-meteo.com](https://open-meteo.com/en/docs/historical-weather-api), one
request per month for 41.01 N, 28.98 E in `Europe/Istanbul` local time.

| Finding | Handling |
|---|---|
| Turkey has been on permanent UTC+3 since 2016, so every month has exactly `days × 24` local hours. | Ingestion logs a warning if the count differs; the Dagster asset check `every_hour_present` and the dbt test `assert_weather_has_no_gaps` fail on gaps. |
| Precipitation is reported as mm in the preceding hour; snowfall in cm. | `is_rainy` = precipitation ≥ 0.1 mm; any snowfall marks the hour as snow. |
| A single grid point represents the whole city, while Istanbul spans ~100 km and two continents. | Stated as a limitation of the analysis. |
