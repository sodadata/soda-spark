from typing import Union
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import DataFrame, Row, SparkSession, types as T

from sodasql.common.yaml_helper import YamlHelper
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.measurement import Measurement
from sodasql.scan.scan import Scan
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_parser import ScanYmlParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml


DEFAULT_TABLE_NAME = "__soda_temporary_view"


class _Warehouse(Warehouse):
    def sql_fetchone(self, sql: str) -> Row:
        """
        Fetch first row of sql output.

        Parameters
        ----------
        sql : str
            The sql to execute.

        Returns
        -------
        out : Row
            The first row.
        """
        out = self.connection.sql(sql)
        return out.first()


class _SparkDialect(SparkDialect):
    def __init__(self) -> None:
        super().__init__(None)
        self.database = "global_temp"

    def create_connection(self) -> SparkSession:
        """
        Create a connection to the spark session.

        Returns
        -------
        out : SparkSession
            The active spark session.
        """
        spark_session = SparkSession.builder.getOrCreate()
        return spark_session

    def sql_columns_metadata(
        self, table_name: str
    ) -> List[Tuple[str, str, str]]:
        """
        Get the meta data for the table.

        Parameters
        ----------
        table_name : str
            The table name.

        Returns
        -------
        out : List[Tuple[str]]
            A list with:
            1) The column name.
            2) The data type.
            3) Nullable or not
        """
        spark_session = self.create_connection()
        response = spark_session.sql(
            f"DESCRIBE TABLE {self.database}.{table_name}"
        )
        return [
            (str(row.col_name), str(row.data_type), "YES")
            for row in response.collect()
        ]


def create_scan_yml(scan_yml_file: Union[str, Path]) -> ScanYml:
    """
    Create a scan yml

    Parameters
    ----------
    scan_yml_file: Union[str, Path]
        The path to a scan file.

    Returns
    -------
    out :
        The scan yml.
    """
    file_system = FileSystemSingleton.INSTANCE
    scan_yml_str = file_system.file_read_as_str(scan_yml_file)
    scan_yml_dict = YamlHelper.parse_yaml(scan_yml_str, scan_yml_file)
    scan_yml_parser = ScanYmlParser(scan_yml_dict, str(scan_yml_file))
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
    warehouse = _Warehouse(warehouse_yml)
    return warehouse


def create_scan(scan_yml: ScanYml) -> Scan:
    """
    Create a scan object

    Parameters
    ----------
    scan_yml : ScanYml
        The scan yml.
    """
    warehouse = create_warehouse()
    scan = Scan(warehouse=warehouse, scan_yml=scan_yml)
    return scan


def pre_execute(scan_yml_file: Union[str, Path], df: DataFrame) -> Scan:
    """
    Function to run before the execute.

    Parameters
    ----------
    scan_yml_file : Union[str, Path]
        The path to a scan file.
    df: DataFrame
        The data frame to be scanned.

    Returns
    -------
    out : Scan
        The scan object.
    """
    scan_yml = create_scan_yml(scan_yml_file)
    scan_yml.table_name = DEFAULT_TABLE_NAME
    df.createOrReplaceGlobalTempView(DEFAULT_TABLE_NAME)
    scan = create_scan(scan_yml)
    return scan


def measurements_to_data_frame(measurements: List[Measurement]) -> DataFrame:
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
        [measurement.to_json() for measurement in measurements], schema=schema
    )
    return out


def execute(scan_yml_file: Union[str, Path], df: DataFrame) -> DataFrame:
    """
    Execute a scan on a data frame.

    Parameters
    ----------
    scan_yml_file : Union[str, Path]
        The path to a scan file.
    df: DataFrame
        The data frame to be scanned.

    Returns
    -------
    out : DataFrame
        The scan results.
    """
    scanner = pre_execute(scan_yml_file, df)
    scanner.execute()
