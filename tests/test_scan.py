import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import pytest
from pandas import testing as pd_testing
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.measurement import Measurement

from sodaspark import scan


@pytest.fixture
def scan_data_frame_path() -> Path:
    return Path(__file__).parent.absolute() / "data/scan_data_frame.yml"


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


def test_create_scan_yml_table_name_is_none(
    scan_data_frame_path: Path,
) -> None:
    """The table name is not defined in the test yaml."""
    scan_yml = scan.create_scan_yml(scan_data_frame_path)
    assert scan_yml.table_name is None


def test_create_warehouse_yml_has_spark_dialect() -> None:
    """The warehouse yml should have the spark dialect"""
    warehouse_yml = scan.create_warehouse_yml()
    assert isinstance(warehouse_yml.dialect, SparkDialect)


def test_create_warehouse_has_spark_dialect(
    spark_session: SparkSession,
) -> None:
    """The warehouse should have the spark dialect"""
    warehouse = scan.create_warehouse()
    assert isinstance(warehouse.dialect, SparkDialect)


def test_create_scan_has_spark_dialect(
    spark_session: SparkSession,
    scan_data_frame_path: Path,
) -> None:
    """The scan should have the spark dialect"""
    scan_yml = scan.create_scan_yml(scan_data_frame_path)
    scanner = scan.create_scan(scan_yml)
    assert isinstance(scanner.dialect, SparkDialect)


def test_scan_execute_data_frame_columns_in_scan_columns(
    spark_session: SparkSession,
    scan_data_frame_path: Path,
    df: DataFrame,
) -> None:
    """
    After the scan execute de data frame columns should be present in the scan
    columns.
    """
    scanner = scan.pre_execute(scan_data_frame_path, df)
    scanner.execute()
    assert all(column in scanner.scan_columns.keys() for column in df.columns)


def test_scan_execute_row_count_in_scan_result_measurements(
    spark_session: SparkSession,
    scan_data_frame_path: Path,
    df: DataFrame,
) -> None:
    """The "row_count" should be in the measurements results."""
    scanner = scan.pre_execute(scan_data_frame_path, df)
    scanner.execute()
    assert any(
        "row_count" == measurement.metric
        for measurement in scanner.scan_result.measurements
    )


def test_measurements_to_data_frame_example(
    spark_session: SparkSession,
) -> None:
    """Convert and valid an example list of measurements."""
    expected = spark_session.createDataFrame(
        [
            {"metric": "metric", "columnName": "id", "value": "10"},
            {"metric": "metric", "columnName": "name", "value": "-30"},
            {"metric": "another_metric", "columnName": "id", "value": "999"},
        ]
    )

    measurements = [
        Measurement(metric="metric", column_name="id", value=10),
        Measurement(metric="metric", column_name="name", value=-30),
        Measurement(metric="another_metric", column_name="id", value=999),
    ]
    out = scan.measurements_to_data_frame(measurements).select(
        *[F.col(column) for column in expected.columns]
    )

    pd_testing.assert_frame_equal(expected.toPandas(), out.toPandas())


def test_scan_execute_gives_row_count_of_five(
    scan_data_frame_path: Path, df: DataFrame
) -> None:
    """The scan execute should give us a row count of five."""

    scan_results = scan.execute(scan_data_frame_path, df)

    row_count = (
        scan_results.where(F.col("metric") == "row_count").first().value
    )

    assert row_count == "6"
