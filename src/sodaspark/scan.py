from typing import Union
from pathlib import Path

from pyspark.sql import DataFrame

from sodasql.common.yaml_helper import YamlHelper
from sodasql.scan.file_system import FileSystemSingleton
from sodasql.scan.scan import Scan
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_parser import ScanYmlParser


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


def create(scan_yml_file: Union[str, Path]) -> Scan:
    """
    Create a scan object

    Parameters
    ----------
    scan_yml_file : Union[str, Path]
        The path to a scan file.
    """
    pass


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
