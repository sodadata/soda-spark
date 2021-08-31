import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession, types as T

from sodaspark.scan import Scan


@pytest.fixture
def scan_data_frame_path() -> Path:
    return Path(__file__).parent.absolute() / "data/scan_data_frame.yml"


@pytest.fixture
def scan(scan_data_frame_path: Path) -> Scan:
    return Scan(scan_data_frame_path)


@dataclass
class Row:
    id: str
    name: str
    size: int
    date: dt.date
    feepct: str
    country: str


@pytest.fixture
def df(spark_session: SparkSession) -> DataFrame:
    """A spark data frame to be used in the tests."""

    id = "a76824f0-50c0-11eb-8be8-88e9fe6293fd"
    data = [
        Row(id, "Paula Landry", 3006, dt.date(2021, 1, 1), "28,42 %", "UK"),
        Row(
            id,
            "Kevin Crawford",
            7243,
            dt.date(2021, 1, 1),
            "22,75 %",
            "Netherlands",
        ),
        Row(id, "Kimberly Green", 6589, dt.date(2021, 1, 1), "11,92 %", "US"),
        Row(id, "William Fox", 1972, dt.date(2021, 1, 1), "14,26 %", "UK"),
        Row(
            id, "Cynthia Gonzales", 3687, dt.date(2021, 1, 1), "18,32 %", "US"
        ),
        Row(id, "Kim Brown", 1277, dt.date(2021, 1, 1), "16,37 %", "US"),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("size", T.IntegerType(), True),
            T.StructField("date", T.DateType(), True),
            T.StructField("feepct", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
        ]
    )
    df = spark_session.createDataFrame(data, schema=schema)
    return df
