from typing import Union
from pathlib import Path

from pyspark.sql import DataFrame


class Scan:
    def __init__(self, scan_yml_path: Union[str, Path]) -> None:
        """
        Scan object.

        Parameters
        ----------
        scan_yml_path : Union[str, Path]
            The path to a scan file.
        """
        pass

    def execute(self, df: DataFrame) -> DataFrame:
        """
        Execute a scan on a data frame.

        Parameters
        ----------
        df: DataFrame
            The data frame to be scanned.

        Returns
        -------
        out : DataFrame
            The scan results.
        """
        pass
