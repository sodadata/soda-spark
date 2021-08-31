from pyspark.sql.session import SparkSession
from soda_spark.example import add, salary_per_department


# We should add more meaningful tests here
def test_make_me_more_meaningful() -> None:
    assert add(2, 3) == 5


def test_salary_per_department(spark_session: SparkSession) -> None:
    df = salary_per_department(
        spark_session, "tests/data/departments.csv", "tests/data/employees.csv"
    )

    assert df.count() == 4  # We expect four departments
    assert df.select("sum_salary").sum() == 2630678  # Based on the inputdata
