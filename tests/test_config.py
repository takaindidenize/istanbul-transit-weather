from pathlib import Path

from transit_weather.config import month_partition_path


def test_month_partition_path():
    assert month_partition_path(Path("raw/ibb"), "2024-03") == Path(
        "raw/ibb/year=2024/month=03/data.parquet"
    )
