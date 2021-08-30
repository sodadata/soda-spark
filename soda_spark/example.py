from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import sum


def add(a: int, b: int) -> int:
    return a + b


def read_csv(spark_session: SparkSession, path: str) -> DataFrame:
    return spark_session.read.load(path, format="csv", inferSchema="true", header="true")


def salary_per_department(
    spark_session: SparkSession, path_employees: str, path_departments: str
) -> DataFrame:
    df_employees = read_csv(spark_session, path_employees)
    df_deployments = read_csv(spark_session, path_departments)
    return (
        df_employees.join(df_deployments, "department_id")
        .groupBy("name")
        .agg(sum("salary").alias("sum_salary"))
    )
