from typing import Union
from pathlib import Path

from pyspark.sql import DataFrame

from sodasql.scan.scan import Scan


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
