from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import TracebackType
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import types as T
from sodasql.common.yaml_helper import YamlHelper
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.scan import Scan
from sodasql.scan.scan_error import ScanError
from sodasql.scan.test_result import TestResult
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.measurement import Measurement
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_parser import ScanYmlParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml
from sodasql.soda_server_client.soda_server_client import SodaServerClient


class Cursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self) -> None:
        self._df: DataFrame | None = None
        self._rows: list[Row] | None = None

    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        """
        The description.

        Returns
        -------
        out : list[tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._df is None:
            description = list()
        else:
            description = [
                (
                    field.name,
                    field.dataType.simpleString(),
                    None,
                    None,
                    None,
                    None,
                    field.nullable,
                )
                for field in self._df.schema.fields
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._df = None
        self._rows = None

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            raise NotImplementedError(
                "Formatting sql statement is not implemented."
            )
        spark_session = SparkSession.builder.getOrCreate()
        self._df = spark_session.sql(sql)

    def fetchall(self) -> list[Row] | None:
        """
        Fetch all data.

        Returns
        -------
        out : list[Row] | None
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()
        return self._rows

    def fetchone(self) -> Row | None:
        """
        Fetch the first output.

        Returns
        -------
        out : Row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()

        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row


class Connection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def cursor(self) -> Cursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return Cursor()


class _SparkDialect(SparkDialect):
    def __init__(self) -> None:
        super().__init__(None)
        self.database = None

    def create_connection(self) -> Connection:
        """
        Create a connection.

        Returns
        -------
        out : Connection
            A connection.
        """
        return Connection()


def create_scan_yml(scan_definition: str | Path) -> ScanYml:
    """
    Create a scan yml

    Parameters
    ----------
    scan_definition: Union[str, Path]
        The path to a scan file or the content of a scan file.

    Returns
    -------
    out :
        The scan yml.
    """
    try:
        is_file = Path(scan_definition).is_file()
    except OSError:
        scan_yml_str = scan_definition
    else:
        if is_file:
            file_system = FileSystemSingleton.INSTANCE
            scan_yml_str = file_system.file_read_as_str(scan_definition)
        else:
            scan_yml_str = scan_definition

    scan_yml_dict = YamlHelper.parse_yaml(scan_yml_str, scan_definition)
    scan_yml_parser = ScanYmlParser(scan_yml_dict, str(scan_definition))
    scan_yml_parser.log()
    scan_yml = scan_yml_parser.scan_yml
    return scan_yml


def create_warehouse_yml() -> WarehouseYml:
    """Create Spark a ware house yml."""
    warehouse_yml = WarehouseYml(
        name="sodaspark",
        dialect=_SparkDialect(),
    )
    return warehouse_yml


def create_warehouse() -> Warehouse:
    """Create a ware house."""
    warehouse_yml = create_warehouse_yml()
    warehouse = Warehouse(warehouse_yml)
    return warehouse


def create_scan(
    scan_yml: ScanYml, soda_server_client: SodaServerClient | None = None
) -> Scan:
    """
    Create a scan object

    Parameters
    ----------
    scan_yml : ScanYml
        The scan yml.
    soda_server_client : Optional[SodaServerClient] (default : None)
        A soda server client.

    Returns
    -------
    out : Scan
        The scan.
    """
    warehouse = create_warehouse()
    scan = Scan(
        warehouse=warehouse,
        scan_yml=scan_yml,
        soda_server_client=soda_server_client,
        time=dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds"),
    )
    return scan


def execute(
    scan_definition: str | Path,
    df: DataFrame,
    *,
    soda_server_client: SodaServerClient | None = None,
) -> ScanResult:
    """
    Execute a scan on a data frame.

    Parameters
    ----------
    scan_definition : Union[str, Path]
        The path to a scan file or the content of a scan file.
    df: DataFrame
        The data frame to be scanned.
    soda_server_client : Optional[SodaServerClient] (default : None)
        A soda server client.

    Returns
    -------
    out : ScanResult
        The scan results.
    """
    scan_yml = create_scan_yml(scan_definition)
    df.createOrReplaceTempView(scan_yml.table_name)
    scan = create_scan(scan_yml, soda_server_client=soda_server_client)
    scan.execute()
    return scan.scan_result

def measurements_to_data_frame(measurements: list[Measurement]) -> DataFrame:
    """
    Convert measurements to a data frame.
    Parameters
    ----------
    measurements: List[Measurement]
        The measurements.
    Returns
    -------
    out : DataFrame
        The measurements as data frame.
    """
    schema_group_values = T.StructType(
        [
            T.StructField("group", T.StringType(), True),
            T.StructField("value", T.StringType(), True),
        ]
    )
    schema = T.StructType(
        [
            T.StructField("metric", T.StringType(), True),
            T.StructField("columnName", T.StringType(), True),
            T.StructField("value", T.StringType(), True),
            T.StructField("groupValues", schema_group_values, True),
        ]
    )
    spark_session = SparkSession.builder.getOrCreate()
    out = spark_session.createDataFrame(
        [measurement.to_dict() for measurement in measurements],
        schema=schema,
    )
    return out


def testresults_to_data_frame(testresults: list[TestResult]) -> DataFrame:
    """
    Convert TestResults to a data frame.
    Parameters
    ----------
    testresults: List[TestResult]
        The testresults.
    Returns
    -------
    out : DataFrame
        The testresults as data frame.
    """
    schema_group_values = T.StructType(
        [
            T.StructField("expression_result", T.LongType(), True),
            T.StructField("row_count", T.LongType(), True),
        ]
    )
    schema = T.StructType(
        [
            T.StructField("columnName", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("expression", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("passed", T.BooleanType(), True),
            T.StructField("skipped", T.BooleanType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField("values", schema_group_values, True),
        ]
    )
    spark_session = SparkSession.builder.getOrCreate()
    out = spark_session.createDataFrame(
        [testresult.to_dict() for testresult in testresults], schema=schema
    )
    return out


def scanerror_to_data_frame(scanerrors: list[ScanError]) -> DataFrame:
    """
    Convert ScanError to a data frame.
    Parameters
    ----------
    scanerror: List[ScanError]
        The scanerrors.
    Returns
    -------
    out : DataFrame
        The scanerrors as data frame.
    """
    schema = T.StructType(
        [
            T.StructField("type", T.StringType(), True),
            T.StructField("message", T.StringType(), True),
        ]
    )
    spark_session = SparkSession.builder.getOrCreate()
    out = spark_session.createDataFrame(
        [scanerror.to_dict() for scanerror in scanerrors], schema=schema
    )
    return out


def convert_scan_result_to_spark_data_frames(
    scan_result: ScanResult,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Convert the scan results to three Spark data frames.
    Parameters
    ------------
    scan_result : ScanResult
        The scan result.
    Returns
    --------
    tuple[DataFrame, DataFrame, DataFrame] :
        A Spark data frame with the:
        1. measurements;
        2. test results;
        3. scan errors;
    """
    return (
        measurements_to_data_frame(scan_result.measurements),
        testresults_to_data_frame(scan_result.test_results),
        scanerror_to_data_frame(scan_result.errors),
    )