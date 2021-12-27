<p align="center"><h1>Soda Spark</h1><br/><b>Data testing, monitoring, and profiling for Spark Dataframes.</b></p>

<p align="center">
  <a href="https://github.com/sodadata/soda-spark/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
  <a href="https://pypi.org/project/soda-spark/"><img alt="Pypi Soda PARK" src="https://img.shields.io/badge/pypi-soda%20spark-green.svg"></a>
  <a href="https://github.com/sodadata/soda-spark/actions/workflows/build.yml"><img alt="Build soda-spark" src="https://github.com/sodadata/soda-spark/actions/workflows/workflow.yml/badge.svg"></a>
</p>


Soda Spark is an extension of
[Soda SQL](https://docs.soda.io/soda-sql/5_min_tutorial.html) that allows you to run Soda
SQL functionality programmatically on a
[Spark data frame](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html).

Soda SQL is an open-source command-line tool. It utilizes user-defined input to prepare SQL queries that run tests on tables in a data warehouse to find invalid, missing, or unexpected data. When tests fail, they surface "bad" data that you can fix to ensure that downstream analysts are using "good" data to make decisions.


## Requirements

Soda Spark has the same requirements as
[`soda-sql-spark`](https://docs.soda.io/soda-sql/installation.html).

## Install

From your shell, execute the following command.

``` sh
$ pip install soda-spark
```

## Use

From your Python prompt, execute the following commands.

``` python
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
... sql_metrics:
... - sql: |
...     SELECT sum(size) as total_size_us
...     FROM demodata
...     WHERE country = 'US'
...   tests:
...   - total_size_us > 5000
... """)
>>> scan_result = scan.execute(scan_definition, df)
>>>
>>> scan_result.measurements  # doctest: +ELLIPSIS
[Measurement(metric='schema', ...), Measurement(metric='row_count', ...), ...]
>>> scan_result.test_results  # doctest: +ELLIPSIS
[TestResult(test=Test(..., expression='row_count > 0', ...), passed=True, skipped=False, ...)]
>>>
```

Or, use a [scan YAML](https://docs.soda.io/soda-sql/scan-yaml.html) file

``` python
>>> scan_yml = "static/demodata.yml"
>>> scan_result = scan.execute(scan_yml, df)
>>>
>>> scan_result.measurements  # doctest: +ELLIPSIS
[Measurement(metric='schema', ...), Measurement(metric='row_count', ...), ...]
>>>
```

See the
[scan result object](https://github.com/sodadata/soda-sql/blob/main/core/sodasql/scan/scan_result.py)
for all attributes and methods.

Or, return Spark data frames:

``` python
>>> measurements, test_results, errors = scan.execute(scan_yml, df, as_frames=True)
>>>
>>> measurements  # doctest: +ELLIPSIS
DataFrame[metric: string, column_name: string, value: string, ...]
>>> test_results  # doctest: +ELLIPSIS
DataFrame[test: struct<...>, passed: boolean, skipped: boolean, values: map<string,string>, ...]
>>>
```

See the `_to_data_frame` functions in the [`scan.py`](./src/sodaspark/scan.py)
to see how the conversion is done.

### Send results to Soda cloud

Send the scan result to Soda cloud.

``` python
>>> import os
>>> from sodasql.soda_server_client.soda_server_client import SodaServerClient
>>>
>>> soda_server_client = SodaServerClient(
...     host="cloud.soda.io",
...     api_key_id=os.getenv("API_PUBLIC"),
...     api_key_secret=os.getenv("API_PRIVATE"),
... )
>>> scan_result = scan.execute(scan_yml, df, soda_server_client=soda_server_client)
>>>
```

## Understand

Under the hood `soda-spark` does the following.

1. Setup the scan
   * Use the Spark dialect
   * Use [Spark session](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html)
     as [warehouse](https://docs.soda.io/soda-sql/warehouse.html) connection
2. Create (or replace)
   [global temporary view](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceGlobalTempView.html)
   for the Spark data frame
3. Execute the scan on the temporary view
