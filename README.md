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
>>> import tempfile
>>> from pyspark.sql import DataFrame, SparkSession
>>> from sodaspark import scan
>>>
>>> spark_session = SparkSession.builder.getOrCreate()
>>>
>>> id = "a76824f0-50c0-11eb-8be8-88e9fe6293fd"
>>> df = spark_session.createDataFrame([
...	   {"id": id, "name": "Paula Landry", "size": 3006},
...	   {"id": id, "name": "Kevin Crawford", "size": 7243}
... ])
>>>
>>> scan_definition = ("""
... table_name: demodata
... metrics:
... - row_count
... - max
... - min_length
... tests:
... - row_count > 0
... columns:
...   id:
...     valid_format: uuid
...     tests:
...     - invalid_percentage == 0
... """)
>>>
>>> with tempfile.NamedTemporaryFile("w+") as temp:
...     _ = temp.write(scan_definition.strip())     # suppress output for doctest
...     _ = temp.seek(0)                            # confirm write for doctest
...     results = scan.execute(temp.name, df)
>>>
>>> results.show()
+------------------+----------+--------------------+-----------+
|            metric|columnName|               value|groupValues|
+------------------+----------+--------------------+-----------+
|            schema|      null|[{logicalType=tex...|       null|
|         row_count|      null|                   2|       null|
|      values_count|        id|                   2|       null|
|       valid_count|        id|                   2|       null|
|        min_length|        id|                  36|       null|
|        min_length|      name|                  12|       null|
|               max|      size|                7243|       null|
|missing_percentage|        id|                 0.0|       null|
|     missing_count|        id|                   0|       null|
| values_percentage|        id|               100.0|       null|
|invalid_percentage|        id|                 0.0|       null|
|     invalid_count|        id|                   0|       null|
|  valid_percentage|        id|               100.0|       null|
+------------------+----------+--------------------+-----------+
<BLANKLINE>
>>>
```
