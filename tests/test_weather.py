from datetime import datetime, timedelta

import httpx
import pyarrow.parquet as pq
import pytest
import respx

from transit_weather import config
from transit_weather.ingestion import weather
from transit_weather.ingestion.months import month_range


def make_payload(hours: int, start: str = "2024-02-01T00:00") -> dict:
    first = datetime.fromisoformat(start)
    times = [(first + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours)]
    hourly = {"time": times}
    for name in config.WEATHER_VARIABLES:
        hourly[name] = [1 if name == "weather_code" else 0.5] * hours
    return {"hourly": hourly}


@pytest.fixture(autouse=True)
def tmp_raw_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "WEATHER_RAW_DIR", tmp_path / "weather")
    monkeypatch.setattr(weather.fetch_month.retry, "sleep", lambda _: None)


def test_month_range_crosses_years():
    assert month_range("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]


@respx.mock
def test_ingest_month_writes_contract(tmp_path):
    route = respx.get(config.OPEN_METEO_ARCHIVE_URL).mock(
        return_value=httpx.Response(200, json=make_payload(29 * 24))
    )
    path = weather.ingest_month("2024-02")

    assert path == tmp_path / "weather" / "year=2024" / "month=02" / "data.parquet"
    params = route.calls.last.request.url.params
    assert params["start_date"] == "2024-02-01"
    assert params["end_date"] == "2024-02-29"
    assert params["timezone"] == config.TIMEZONE
    table = pq.read_table(path)
    assert table.schema.equals(weather.SCHEMA)
    assert table.num_rows == 29 * 24


@respx.mock
def test_retries_after_server_error():
    respx.get(config.OPEN_METEO_ARCHIVE_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json=make_payload(29 * 24))]
    )
    assert weather.ingest_month("2024-02") is not None


@respx.mock
def test_client_error_is_not_retried():
    route = respx.get(config.OPEN_METEO_ARCHIVE_URL).mock(return_value=httpx.Response(400))
    with pytest.raises(httpx.HTTPStatusError):
        weather.ingest_month("2024-02")
    assert route.call_count == 1


def test_gaps_and_nulls_are_logged_not_fatal(caplog):
    payload = make_payload(10)
    payload["hourly"]["temperature_2m"][3] = None
    table = weather.to_table(payload, "2024-02")

    assert table.num_rows == 10
    assert table.column("temperature_2m").null_count == 1
    assert "expected 696" in caplog.text
    assert "null values in temperature_2m" in caplog.text
