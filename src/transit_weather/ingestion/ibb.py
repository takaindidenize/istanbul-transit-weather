"""Ingest İBB hourly public transport CSVs into raw Parquet, one partition per month.

Usage: python -m transit_weather.ingestion.ibb --start 2024-01 --end 2024-12
"""

import argparse
import logging
import re
import tempfile
import unicodedata
from pathlib import Path

import duckdb
import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from transit_weather import config
from transit_weather.ingestion.months import month_range

log = logging.getLogger(__name__)

# Raw contract: column name -> DuckDB type (see CLAUDE.md).
CONTRACT = {
    "transition_date": "DATE",
    "transition_hour": "TINYINT",
    "transport_type_id": "TINYINT",
    "road_type": "VARCHAR",
    "line": "VARCHAR",
    "transfer_type": "VARCHAR",
    "number_of_passage": "INTEGER",
    "number_of_passenger": "INTEGER",
    "product_kind": "VARCHAR",
    "transaction_type_desc": "VARCHAR",
    "town": "VARCHAR",
    "line_name": "VARCHAR",
    "station_poi_desc_cd": "VARCHAR",
}

# Known or plausible header variants, after lower-casing and replacing spaces with underscores.
COLUMN_ALIASES = {
    "date": "transition_date",
    "hour": "transition_hour",
    "number_of_passages": "number_of_passage",
    "number_of_passengers": "number_of_passenger",
    "station_poi_desc": "station_poi_desc_cd",
}

TURKISH_MONTHS = {
    "ocak": 1,
    "subat": 2,
    "mart": 3,
    "nisan": 4,
    "mayis": 5,
    "haziran": 6,
    "temmuz": 7,
    "agustos": 8,
    "eylul": 9,
    "ekim": 10,
    "kasim": 11,
    "aralik": 12,
}


def _ascii_fold(text: str) -> str:
    text = text.replace("ı", "i").replace("İ", "I")
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()


def month_from_name(name: str) -> str | None:
    """'Aralık 2024 Toplu Ulaşım Verisi' -> '2024-12'."""
    match = re.match(r"\s*(\w+)\s+(\d{4})", _ascii_fold(name))
    if not match or match.group(1) not in TURKISH_MONTHS:
        return None
    return f"{match.group(2)}-{TURKISH_MONTHS[match.group(1)]:02d}"


def month_from_url(url: str) -> str | None:
    """'.../hourly_transportation_202401.csv' -> '2024-01'."""
    match = re.search(r"(\d{4})(\d{2})\.csv$", url)
    return f"{match.group(1)}-{match.group(2)}" if match else None


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError)


_retry = retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, max=60),
    reraise=True,
)


@_retry
def list_months(client: httpx.Client | None = None) -> dict[str, str]:
    """Map 'YYYY-MM' to CSV download URL, using the Turkish resource name as the source of truth."""
    owns_client = client is None
    client = client or httpx.Client(timeout=60)
    try:
        response = client.get(config.IBB_CKAN_URL, params={"id": config.IBB_DATASET_ID})
        response.raise_for_status()
        resources = response.json()["result"]["resources"]
    finally:
        if owns_client:
            client.close()

    months: dict[str, str] = {}
    for resource in resources:
        name, url = resource.get("name", ""), resource.get("url", "")
        month = month_from_name(name)
        if month is None:
            log.warning("ibb: cannot parse month from resource name %r, skipping", name)
            continue
        url_month = month_from_url(url)
        if url_month != month:
            log.warning("ibb: resource %r has URL month %s, using %s", name, url_month, month)
        if month in months:
            log.warning("ibb: duplicate resource for %s, keeping the first", month)
            continue
        months[month] = url
    return months


@_retry
def download(url: str, dest: Path, client: httpx.Client) -> None:
    """Stream a URL to a file without holding it in memory."""
    with client.stream("GET", url, follow_redirects=True) as response:
        response.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in response.iter_bytes(chunk_size=1 << 20):
                fh.write(chunk)


def normalise_column(name: str) -> str:
    key = re.sub(r"\s+", "_", name.strip().lower())
    return COLUMN_ALIASES.get(key, key)


def csv_to_parquet(csv_path: Path, out_path: Path, month: str) -> int:
    """Convert one source CSV to the raw contract. Returns the row count (0 = nothing written)."""
    con = duckdb.connect()
    try:
        con.execute("SET preserve_insertion_order = false")
        relation = con.read_csv(str(csv_path), header=True, all_varchar=True)
        present = {normalise_column(col): col for col in relation.columns}

        selects = []
        for column, dtype in CONTRACT.items():
            if column in present:
                source = present[column].replace('"', '""')
                selects.append(f'TRY_CAST(trim("{source}") AS {dtype}) AS {column}')
            else:
                log.warning("ibb %s: column %s missing in source, filled with NULL", month, column)
                selects.append(f"CAST(NULL AS {dtype}) AS {column}")
        unexpected = sorted(set(present) - set(CONTRACT))
        if unexpected:
            log.warning("ibb %s: ignoring unexpected columns %s", month, unexpected)
        selects.append(f"'{month}' AS source_month")
        selects.append("CAST(date_trunc('second', now()) AS TIMESTAMP) AS loaded_at")

        query = f"SELECT {', '.join(selects)} FROM read_csv(?, header = true, all_varchar = true)"
        rows = con.execute(f"SELECT count(*) FROM ({query})", [str(csv_path)]).fetchone()[0]
        if rows == 0:
            return 0

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_out = out_path.with_suffix(".parquet.tmp")
        escaped_out = str(tmp_out).replace("'", "''")
        con.execute(
            f"COPY ({query}) TO '{escaped_out}' (FORMAT parquet, COMPRESSION zstd)",
            [str(csv_path)],
        )
        tmp_out.replace(out_path)

        bad = con.execute(
            f"SELECT count(*) FROM ({query}) WHERE transition_date IS NULL "
            "OR transition_hour IS NULL OR transport_type_id IS NULL",
            [str(csv_path)],
        ).fetchone()[0]
        if bad:
            log.warning("ibb %s: %d rows with unparseable date, hour or transport type", month, bad)
        return rows
    finally:
        con.close()


def ingest_month(
    month: str,
    client: httpx.Client | None = None,
    months: dict[str, str] | None = None,
) -> Path | None:
    """Download one month and (over)write its raw Parquet partition.

    Returns the partition path, or None when the source month is missing or empty.
    """
    owns_client = client is None
    client = client or httpx.Client(timeout=httpx.Timeout(60, read=300))
    try:
        months = months if months is not None else list_months(client)
        url = months.get(month)
        if url is None:
            log.warning("ibb %s: no source resource, skipping", month)
            return None

        out_path = config.month_partition_path(config.IBB_RAW_DIR, month)
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=config.DATA_DIR) as tmp:
            csv_path = Path(tmp) / f"{month}.csv"
            log.info("ibb %s: downloading %s", month, url)
            download(url, csv_path, client)
            rows = csv_to_parquet(csv_path, out_path, month)

        if rows == 0:
            log.warning("ibb %s: source file has no data rows, skipping", month)
            out_path.unlink(missing_ok=True)
            return None
        log.info("ibb %s: wrote %d rows to %s", month, rows, out_path)
        return out_path
    finally:
        if owns_client:
            client.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=config.START_MONTH)
    parser.add_argument("--end", default=config.END_MONTH)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with httpx.Client(timeout=httpx.Timeout(60, read=300)) as client:
        months = list_months(client)
        for month in month_range(args.start, args.end):
            ingest_month(month, client, months)


if __name__ == "__main__":
    main()
