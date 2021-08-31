# Soda Spark

Soda Spark is an open-source data quality tool for Spark data frames. It is an
extension of [soda-sql](https://github.com/sodadata/soda-sql) that allows
you to run Soda SQL functionality against a Spark data frame.

# Install Soda Spark

Install the package using pip.

``` sh
pip install soda-spark
```

# Use Soda Spark

[Intall Soda Spark](#install-soda-spark), then execute a scan with:

``` python
from pyspark.sql import DataFrame
from sodaspark import scan

df: DataFrame = ...

scan = scan.execute("./tables/dataframe.yml", df)
```

To compute a single metric, do the following:

``` python
from pyspark.sql import DataFrame
import sodaspark

df: DataFrame = ...

sodaspark.get_metric(df, "row_count")
```
