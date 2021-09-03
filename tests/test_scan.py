import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession, types as T
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.measurement import Measurement

from sodaspark import scan


SCAN_CONTENT = """
table_name: demodata
metrics:
    - row_count
    - missing_count
    - missing_percentage
    - values_count
    - values_percentage
    - valid_count
    - valid_percentage
    - invalid_count
    - invalid_percentage
    - min_length
    - max_length
    - avg_length
    - min
    - max
    - avg
    - sum
    - variance
    - stddev
tests:
    - row_count > 0
columns:
    id:
        valid_format: uuid
        tests:
        - invalid_percentage == 0
    feepct:
        valid_format: number_percentage
        tests:
        - invalid_percentage == 0
"""


@pytest.fixture
def scan_data_frame_path(tmp_path: Path) -> Path:
    scan_path = tmp_path / "table.yml"
    with scan_path.open("w") as f:
        f.write(SCAN_CONTENT.strip())
    return scan_path


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

    date = dt.date(2021, 1, 1)
    id = "a76824f0-50c0-11eb-8be8-88e9fe6293fd"
    data = [
        Row(id, "Paula Landry", 3006, date, "28,42 %", "UK"),
        Row(id, "Kevin Crawford", 7243, date, "22,75 %", "NL"),
        Row(id, "Kimberly Green", 6589, date, "11,92 %", "US"),
        Row(id, "William Fox", 1972, date, "14,26 %", "UK"),
        Row(id, "Cynthia Gonzales", 3687, date, "18,32 %", "US"),
        Row(id, "Kim Brown", 1277, date, "16,37 %", "US"),
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


def test_create_scan_yml_table_name_is_demodata(
    scan_data_frame_path: Path,
) -> None:
    """Validate the table name is as expected."""
    scan_yml = scan.create_scan_yml(scan_data_frame_path)
    assert scan_yml.table_name == "demodata"


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


def test_scan_execute_data_frame_columns_in_scan_result_measurements(
    spark_session: SparkSession,
    scan_data_frame_path: Path,
    df: DataFrame,
) -> None:
    """We expect the columns to be present in the scan result measurements."""
    scan_result = scan.execute(scan_data_frame_path, df)
    scan_result_columns = set(
        measurement.column_name for measurement in scan_result.measurements
    )
    assert len(set(df.columns) - scan_result_columns) == 0


def is_equal_or_both_none(left: Any, right: Any) -> bool:
    """
    Check if left and right are equal or both None.

    Parameters
    ----------
    left: Any :
        Right element.
    right: Any
        Left element.

    Returns
    -------
    out : bool
        True, if the left and right are equal or both None. False, otherwise.
    """
    return (left == right) or (left is None and right is None)


def is_same_measurement(left: Measurement, right: Measurement) -> bool:
    """
    Check if the measurements are the same.

    Parameters
    ----------
    left : Measurement
        The left measurement.
    right : Measurement
        The right measurement.

    Returns
    -------
    out : bool
        True if the measurements are the same, false otherwise.
    """
    return (
        left.metric == right.metric
        and is_equal_or_both_none(left.column_name, right.column_name)
        and is_equal_or_both_none(left.value, right.value)
        and is_equal_or_both_none(left.group_values, right.group_values)
    )


@pytest.mark.parametrize(
    "measurement",
    [
        Measurement(metric="row_count", column_name=None, value=6),
    ],
)
def test_scan_execute_contains_expected_metric(
    scan_data_frame_path: Path,
    df: DataFrame,
    measurement: Measurement,
) -> None:
    """Valid if the expect measurements are present."""

    scan_result = scan.execute(scan_data_frame_path, df)

    assert any(
        is_same_measurement(measurement, output_measurement)
        for output_measurement in scan_result.measurements
    )
