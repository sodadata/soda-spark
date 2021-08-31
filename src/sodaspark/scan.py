from pyspark.sql import DataFrame


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
