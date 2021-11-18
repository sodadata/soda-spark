from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import BinaryIO

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T  # noqa: N812
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.measurement import Measurement
from sodasql.scan.test import Test
from sodasql.scan.test_result import TestResult
from sodasql.soda_server_client.soda_server_client import SodaServerClient

from sodaspark import scan

SCAN_DEFINITION = """
table_name: demodata
samples:
  table_limit: 50
  failed_limit: 50
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
sql_metrics:
- sql: |
    SELECT sum(size) as total_size_us
    FROM demodata
    WHERE country = 'US'
  tests:
  - total_size_us > 5000
"""


class MockSodaServerClient(SodaServerClient):
    """
    Source
    ------
    https://github.com/sodadata/soda-sql/blob/main/tests/common/mock_soda_server_client.py
    """

    # noinspection PyMissingConstructor
    def __init__(self) -> None:
        self.host: str = "MockSodaServerClient"
        self.token: str = "mocktoken"
        self.file_uploads: dict = {}

    def execute_command(self, command: dict) -> dict | list[dict] | None:
        # Serializing is important as it ensures no exceptions occur during serialization
        json.dumps(command, indent=2)
        # Still we use the unserialized version to check the results as that is easier

        out: dict | list[dict] | None = None
        if command["type"] == "sodaSqlScanStart":
            out = {"scanReference": "scanref-123"}
        elif command["type"] == "sodaSqlCustomMetrics":
            out = [
                {
                    "id": "f255b6af-f2ad-485c-8222-416ccbe4b6e2",
                    "type": "missingValuesCount",
                    "columnName": "id",
                    "datasetId": "901d99c4-2dfe-43f9-acf3-f0344fc690a0",
                    "filter": {
                        "type": "equals",
                        "left": {"type": "columnValue", "columnName": "date"},
                        "right": {"type": "time", "scanTime": True},
                    },
                    "custom": True,
                }
            ]
        return out

    def execute_query(self, command: dict) -> list[dict]:
        if command["type"] == "sodaSqlCustomMetrics":
            return [
                {
                    "id": "f255b6af-f2ad-485c-8222-416ccbe4b6e2",
                    "type": "missingValuesCount",
                    "columnName": "id",
                    "datasetId": "901d99c4-2dfe-43f9-acf3-f0344fc690a0",
                    "filter": {
                        "type": "greaterThanOrEqual",
                        "left": {"type": "columnValue", "columnName": "date"},
                        "right": {"type": "time", "scanTime": True},
                    },
                    "custom": True,
                }
            ]

        raise RuntimeError(f"{command['type']} is not supported yet")

    def _upload_file(self, headers: str, temp_file: BinaryIO) -> dict:
        file_id = f"file-{str(len(self.file_uploads))}"
        data = temp_file.read().decode("utf-8")
        self.file_uploads[file_id] = {"headers": headers, "data": data}
        temp_file.close()
        return {"fileId": file_id}


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
        measurement == output_measurement
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
        measurement == output_measurement
        for output_measurement in scan_result.measurements
    )


@pytest.mark.parametrize(
    "test_result",
    [
        TestResult(
            test=Test(
                id='{"expression":"row_count > 0"}',
                title="test(row_count > 0)",
                expression="row_count > 0",
                metrics=["row_count"],
                column=None,
            ),
            passed=True,
            skipped=False,
            values={"expression_result": 6, "row_count": 6},
            error=None,
            group_values=None,
        ),
        TestResult(
            test=Test(
                id='{"column":"id","expression":"invalid_percentage == 0"}',
                title="column(id) test(invalid_percentage == 0)",
                expression="invalid_percentage == 0",
                metrics=["invalid_percentage"],
                column="id",
            ),
            passed=True,
            skipped=False,
            values={"expression_result": 0.0, "invalid_percentage": 0.0},
            error=None,
        ),
        TestResult(
            test=Test(
                id='{"column":"feepct","expression":"invalid_percentage == 0"}',
                title="column(feepct) test(invalid_percentage == 0)",
                expression="invalid_percentage == 0",
                metrics=["invalid_percentage"],
                column="feepct",
            ),
            passed=True,
            skipped=False,
            values={"expression_result": 0.0, "invalid_percentage": 0.0},
            error=None,
            group_values=None,
        ),
        TestResult(
            test=Test(
                id='{"sql_metric_index":0,"expression":"total_size_us > 5000"}',
                title="sqlmetric(0) test(total_size_us > 5000)",
                expression="total_size_us > 5000",
                metrics=["total_size_us"],
                column=None,
            ),
            passed=True,
            skipped=False,
            values={"expression_result": 11553, "total_size_us": 11553},
            error=None,
            group_values=None,
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
        test_result == output_test_result
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


def test_scan_execute_with_soda_server_client_scan_result_does_not_contain_any_errors(
    scan_definition: str,
    df: DataFrame,
) -> None:
    """
    The scan results should not contain any erros, also not when there is
    Soda server client.
    """
    soda_server_client = MockSodaServerClient()
    scan_result = scan.execute(
        scan_definition, df, soda_server_client=soda_server_client
    )

    assert not scan_result.has_errors()
