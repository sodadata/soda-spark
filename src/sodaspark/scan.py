from typing import Iterator

from pyspark.sql import DataFrame
from soda.scan.column_metadata import ColumnMetadata


def get_metric(df: DataFrame, metric: str) -> DataFrame:
    """
    Get the value of a metric - a measurement.

    Parameters
    ----------
    df: DataFrame :
       The data frame.
    metric: str
       The name of the metric.

    Returns
    -------
    out : DataFrame
        The measurement with its value.
    """
    pass


def iterate_column_metadata(*columns: list[str]) -> Iterator[ColumnMetadata]:
    """
    Iterate over the column meta data objects.

    Parameters
    ----------
    *columns: List[str]
        The columns to iterate over.

    Returns
    -------
    out : ColumnMetadata
        The column metadata.
    """
    pass


def compile_sql_for_metric(metric: str) -> str:
    """
    Compile the SQL for a metric.

    Parameters
    ----------
    metric: str
        The metric.

    Returns
    -------
    out : str
        The compiled SQL.
    """
    pass
