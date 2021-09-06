import datetime as dt
from dataclasses import dataclass
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T  # noqa: N812
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.measurement import Measurement
from sodasql.scan.test import Test
from sodasql.scan.test_result import TestResult

from sodaspark import scan

SCAN_DEFINITION = """
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
metric_groups:
- duplicates
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
excluded_columns:
- date
"""


@pytest.fixture
def scan_definition() -> str:
    return SCAN_DEFINITION


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
    scan_definition: str,
) -> None:
    """Validate the table name is as expected."""
    scan_yml = scan.create_scan_yml(scan_definition)
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
    scan_definition: str,
) -> None:
    """The scan should have the spark dialect"""
    scan_yml = scan.create_scan_yml(scan_definition)
    scanner = scan.create_scan(scan_yml)
    assert isinstance(scanner.dialect, SparkDialect)


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
        Measurement(metric="valid_count", column_name="id", value=6),
        Measurement(metric="max_length", column_name="name", value=16),
        Measurement(metric="sum", column_name="size", value=23774),
    ],
)
def test_scan_execute_contains_expected_metric(
    scan_definition: str,
    df: DataFrame,
    measurement: Measurement,
) -> None:
    """Valid if the expected measurement is present."""

    scan_result = scan.execute(scan_definition, df)

    assert any(
        is_same_measurement(measurement, output_measurement)
        for output_measurement in scan_result.measurements
    )


@pytest.mark.parametrize(
    "measurement",
    [
        Measurement(metric="distinct", column_name="country", value=3),
    ],
)
def test_scan_execute_with_metric_groups_measurement_as_expected(
    scan_definition: str,
    df: DataFrame,
    measurement: Measurement,
) -> None:
    """Valid if the expected measurement is present."""

    scan_result = scan.execute(scan_definition, df)

    assert any(
        is_same_measurement(measurement, output_measurement)
        for output_measurement in scan_result.measurements
    )


def is_same_test(left: Test, right: Test) -> bool:
    """
    Check if the test are the same.

    Parameters
    ----------
    left : Test
        The left test.
    right : TestResult
        The right test.

    Returns
    -------
    out : bool
        True if the tests are the same, false otherwise.
    """
    return is_equal_or_both_none(
        left.expression, right.expression
    ) and is_equal_or_both_none(left.column, right.column)


def is_same_test_result(left: TestResult, right: TestResult) -> bool:
    """
    Check if the test results are the same.

    Parameters
    ----------
    left : TestResult
        The left test result.
    right : TestResult
        The right test result.

    Returns
    -------
    out : bool
        True if the tests results are the same, false otherwise.
    """
    return (
        is_same_test(left.test, right.test)
        and is_equal_or_both_none(left.passed, right.passed)
        and is_equal_or_both_none(left.skipped, right.skipped)
        and is_equal_or_both_none(left.values, right.values)
        and is_equal_or_both_none(left.error, right.error)
        and is_equal_or_both_none(left.group_values, right.group_values)
    )


@pytest.mark.parametrize(
    "test_result",
    [
        TestResult(
            test=Test(
                id=None,
                title=None,
                expression="row_count > 0",
                column=None,
                metrics=[],
            ),
            passed=True,
            skipped=False,
            values={"row_count": 6},
            error=None,
        ),
        TestResult(
            test=Test(
                id=None,
                title=None,
                expression="invalid_percentage == 0",
                column="id",
                metrics=[],
            ),
            passed=True,
            skipped=False,
            values={"invalid_percentage": 0.0},
            error=None,
        ),
        TestResult(
            test=Test(
                id=None,
                title=None,
                expression="invalid_percentage == 0",
                metrics=[],
                column="feepct",
            ),
            passed=True,
            skipped=False,
            values={"invalid_percentage": 0.0},
            error=None,
        ),
    ],
)
def test_scan_execute_contains_expected_test_result(
    scan_definition: str,
    df: DataFrame,
    test_result: Test,
) -> None:
    """Valid if the expected test result is present."""

    scan_result = scan.execute(scan_definition, df)

    assert any(
        is_same_test_result(test_result, output_test_result)
        for output_test_result in scan_result.test_results
    )


def test_scan_execute_scan_result_does_not_contain_any_errors(
    scan_definition: str,
    df: DataFrame,
) -> None:
    """The scan results should not contain any erros."""

    scan_result = scan.execute(scan_definition, df)

    assert not scan_result.has_errors()


def test_excluded_columns_date_is_not_present(
    scan_definition: str,
    df: DataFrame,
) -> None:
    """The date column should not be present in the measurements."""

    scan_result = scan.execute(scan_definition, df)

    assert not any(
        measurement.column_name == "date"
        for measurement in scan_result.measurements
    )
