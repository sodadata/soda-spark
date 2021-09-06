from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from sodasql.common.yaml_helper import YamlHelper
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.scan import Scan
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_parser import ScanYmlParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml


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
    ) -> list[tuple[str, str, str]]:
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


def execute(scan_definition: str | Path, df: DataFrame) -> ScanResult:
    """
    Execute a scan on a data frame.

    Parameters
    ----------
    scan_definition : Union[str, Path]
        The path to a scan file or the content of a scan file.
    df: DataFrame
        The data frame to be scanned.

    Returns
    -------
    out : ScanResult
        The scan results.
    """
    scan_yml = create_scan_yml(scan_definition)
    df.createOrReplaceGlobalTempView(scan_yml.table_name)
    scan = create_scan(scan_yml)
    scan.execute()
    return scan.scan_result
