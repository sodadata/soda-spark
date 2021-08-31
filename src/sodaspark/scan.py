from typing import Union
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sodasql.common.yaml_helper import YamlHelper
from sodasql.dialects.spark_dialect import SparkDialect
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.scan import Scan
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_parser import ScanYmlParser
from sodasql.scan.warehouse import Warehouse
from sodasql.scan.warehouse_yml import WarehouseYml


class _SparkDialect(SparkDialect):
    def __init__(self) -> None:
        super().__init__(None)
        self.database = "default"

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
        dialect=_SparkDialect(),
    )
    return warehouse_yml


def create_warehouse() -> Warehouse:
    """Create a ware house."""
    warehouse_yml = create_warehouse_yml()
    warehouse = Warehouse(warehouse_yml)
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
    pass
