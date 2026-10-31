import logging
from datetime import date

import duckdb
import httpx
import pytest
import respx

from transit_weather import config
from transit_weather.ingestion import ibb

HEADER = (
    "transition_date,transition_hour,transport_type_id,road_type,line,transfer_type,"
    "number_of_passage,number_of_passenger,product_kind,transaction_type_desc,town,line_name,"
    "station_poi_desc_cd"
)
ROWS = [
    "2024-01-01,00,2,RAYLI,TOPKAPI-HABIBLER,Normal,31,21,TAM,Tam Kontur,BAYRAMPASA,T4,CUKURCESME",
    "2024-01-01,07,1,OTOYOL,500T,Aktarma,120,98,INDIRIMLI1,Indirimli Kontur,FATIH,500T,",
    "2024-01-01,23,3,DENIZ,KADIKOY-KARAKOY,Normal,5,5,TAM,Tam Kontur,KADIKOY,VAPUR,KADIKOY",
]
BASE = "https://data.ibb.gov.tr/dataset/x/resource"


def resource(name: str, filename: str) -> dict:
    return {"name": name, "url": f"{BASE}/{filename}"}


@pytest.fixture(autouse=True)
def tmp_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "IBB_RAW_DIR", tmp_path / "raw" / "ibb")
    for fn in (ibb.list_months, ibb.download):
        monkeypatch.setattr(fn.retry, "sleep", lambda _: None)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Ocak 2020 Toplu Ulaşım Verisi", "2020-01"),
        ("Şubat 2021 Toplu Ulaşım Verisi", "2021-02"),
        ("Ağustos 2023 Toplu Ulaşım Verisi", "2023-08"),
        ("Aralık 2024 Toplu Ulaşım Verisi", "2024-12"),
        ("Mayis 2022", "2022-05"),
        ("Data dictionary", None),
    ],
)
def test_month_from_name(name, expected):
    assert ibb.month_from_name(name) == expected


@respx.mock
def test_list_months_trusts_name_over_filename(caplog):
    respx.get(config.IBB_CKAN_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "result": {
                    "resources": [
                        resource(
                            "Kasım 2024 Toplu Ulaşım Verisi", "hourly_transportation_202411.csv"
                        ),
                        resource(
                            "Aralık 2024 Toplu Ulaşım Verisi", "hourly_transportation_202512.csv"
                        ),
                        resource("Veri sözlüğü", "dictionary.pdf"),
                    ]
                }
            },
        )
    )
    with caplog.at_level(logging.WARNING):
        months = ibb.list_months()

    assert months == {
        "2024-11": f"{BASE}/hourly_transportation_202411.csv",
        "2024-12": f"{BASE}/hourly_transportation_202512.csv",
    }
    assert "URL month 2025-12, using 2024-12" in caplog.text


@respx.mock
def test_ingest_month_writes_contract():
    url = f"{BASE}/hourly_transportation_202401.csv"
    respx.get(url).mock(return_value=httpx.Response(200, text="\n".join([HEADER, *ROWS]) + "\n"))

    path = ibb.ingest_month("2024-01", months={"2024-01": url})

    assert path == config.IBB_RAW_DIR / "year=2024" / "month=01" / "data.parquet"
    con = duckdb.connect()
    schema = {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()}
    assert schema["transition_date"] == "DATE"
    assert schema["transition_hour"] == "TINYINT"
    assert schema["number_of_passenger"] == "INTEGER"
    assert schema["source_month"] == "VARCHAR"
    assert schema["loaded_at"] == "TIMESTAMP"
    rows = con.execute(
        f"SELECT transition_date, transition_hour, transport_type_id, number_of_passage, "
        f"station_poi_desc_cd FROM '{path}' ORDER BY transition_hour"
    ).fetchall()
    assert rows == [
        (date(2024, 1, 1), 0, 2, 31, "CUKURCESME"),
        (date(2024, 1, 1), 7, 1, 120, None),
        (date(2024, 1, 1), 23, 3, 5, "KADIKOY"),
    ]


@respx.mock
def test_drifted_header_is_normalised(caplog):
    url = f"{BASE}/hourly_transportation_202402.csv"
    drifted = HEADER.replace("number_of_passenger", "Number Of Passengers").replace(
        ",station_poi_desc_cd", ""
    )
    body = "\n".join([drifted, *(r.rsplit(",", 1)[0] for r in ROWS)]) + "\n"
    respx.get(url).mock(return_value=httpx.Response(200, text=body))

    with caplog.at_level(logging.WARNING):
        path = ibb.ingest_month("2024-02", months={"2024-02": url})

    con = duckdb.connect()
    passengers, stations = con.execute(
        f"SELECT sum(number_of_passenger), count(station_poi_desc_cd) FROM '{path}'"
    ).fetchone()
    assert passengers == 124
    assert stations == 0
    assert "column station_poi_desc_cd missing" in caplog.text


@respx.mock
def test_empty_month_is_skipped():
    url = f"{BASE}/hourly_transportation_202411.csv"
    respx.get(url).mock(return_value=httpx.Response(200, text=HEADER + "\n"))

    assert ibb.ingest_month("2024-11", months={"2024-11": url}) is None
    assert not (config.IBB_RAW_DIR / "year=2024" / "month=11" / "data.parquet").exists()


def test_unknown_month_is_skipped():
    assert ibb.ingest_month("2019-01", months={}) is None


@respx.mock
def test_download_retries_after_server_error():
    url = f"{BASE}/hourly_transportation_202401.csv"
    respx.get(url).mock(
        side_effect=[httpx.Response(502), httpx.Response(200, text=HEADER + "\n" + ROWS[0] + "\n")]
    )
    assert ibb.ingest_month("2024-01", months={"2024-01": url}) is not None
