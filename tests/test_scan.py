from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import BinaryIO

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.group_value import GroupValue
from sodasql.scan.measurement import Measurement
from sodasql.scan.scan_error import TestExecutionScanError
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
- sql: |
    SELECT country, count(id) as country_count
    FROM demodata
    GROUP BY country
  group_fields:
  - country
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
class Record:
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
        Record(id, "Paula Landry", 3006, date, "28,42 %", "UK"),
        Record(id, "Kevin Crawford", 7243, date, "22,75 %", "NL"),
        Record(id, "Kimberly Green", 6589, date, "11,92 %", "US"),
        Record(id, "William Fox", 1972, date, "14,26 %", "UK"),
        Record(id, "Cynthia Gonzales", 3687, date, "18,32 %", "US"),
        Record(id, "Kim Brown", 1277, date, "16,37 %", "US"),
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


def test_scan_execute_scan_result_does_not_contain_any_errors(
    scan_definition: str,
    df: DataFrame,
) -> None:
    """The scan results should not contain any erros."""

    scan_result = scan.execute(scan_definition, df)

    assert not scan_result.has_errors()


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


def test_measurements_to_data_frame(spark_session: SparkSession) -> None:
    """
    Test conversions of measurements to dataframe.

    A failure of this test indicates that the `Measurement` dataclass has been
    changed in `soda-sql`. If a failure happens, the code needs to be updated to
    accomodate for that change. Start with updating the expected output data
    frame in this test, then change the schema used for converting the
    measurements.
    """
    expected = spark_session.createDataFrame(
        [
            Row(
                metric="values_count",
                column_name="officename",
                value="",
                group_values=[
                    Row(group={"statename": "statename"}, value="9872")
                ],
            )
        ]
    ).withColumn(
        "value", F.when(F.col("value") == "", None).otherwise(F.col("value"))
    )

    measurements = [
        Measurement(
            metric="values_count",
            column_name="officename",
            value=None,
            group_values=[
                GroupValue(group={"statename": "statename"}, value="9872")
            ],
        )
    ]
    out = scan.measurements_to_data_frame(measurements)
    assert expected.collect() == out.collect()


def test_test_results_to_data_frame(spark_session: SparkSession) -> None:
    """
    Test conversions of test_result to dataframe.

    A failure of this test indicates that the `TestResult` dataclass has been
    changed in `soda-sql`. If a failure happens, the code needs to be updated to
    accomodate for that change. Start with updating the expected output data
    frame in this test, then change the schema used for converting the test
    results.
    """
    expected = spark_session.createDataFrame(
        [
            Row(
                test=Row(
                    id="id",
                    title="title",
                    expression="expression",
                    metrics=["metrics"],
                    column="column",
                    source="source",
                ),
                passed=True,
                skipped=False,
                values={"value": "10"},
                error="exception",
                group_values={"group": "by"},
            )
        ]
    )

    test_results = [
        TestResult(
            Test(
                id="id",
                title="title",
                expression="expression",
                metrics=["metrics"],
                column="column",
                source="source",
            ),
            passed=True,
            skipped=False,
            values={"value": 10},
            error="exception",
            group_values={"group": "by"},
        )
    ]
    out = scan.test_results_to_data_frame(test_results)
    assert expected.collect() == out.collect()


def test_scan_error_to_data_frame(spark_session: SparkSession) -> None:
    """
    Test conversions of scan_error to dataframe.

    A failure of this test indicates that the `ScanError` dataclass has been
    changed in `soda-sql`. If a failure happens, the code needs to be updated to
    accomodate for that change. Start with updating the expected output data
    frame in this test, then change the schema used for converting the scan
    error.
    """
    expected = spark_session.createDataFrame(
        [
            Row(
                message='Test "metric_name > 30" failed',
                exception="name 'metric_name' is not defined",
            )
        ]
    )

    scan_errors = [
        TestExecutionScanError(
            message='Test "metric_name > 30" failed',
            exception="name 'metric_name' is not defined",
        )
    ]
    out = scan.scan_errors_to_data_frame(scan_errors)
    assert expected.collect() == out.collect()


def test_scan_execute_return_as_data_frame(
    scan_definition: str, df: DataFrame
) -> None:
    """Valid if row and column count match."""

    scan_result = scan.execute(scan_definition, df, as_frames=True)
    # Comparing rowcount and columncount of Dataframes for the scan_definition
    assert ((88, 4), (4, 6), (0, 2)) == (
        (scan_result[0].count(), len(scan_result[0].columns)),
        (scan_result[1].count(), len(scan_result[1].columns)),
        (scan_result[2].count(), len(scan_result[2].columns)),
    )
