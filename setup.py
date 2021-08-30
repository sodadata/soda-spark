from setuptools import find_packages, setup

setup(
    name="soda-spark",
    packages=find_packages(),
    version="0.1.0",
    description="Soda SQL API for PySpark data frame",
    author="Soda",
    extras_require={
        "dev": ["pre-commit==2.2.0", "pytest-spark==0.6.0", "pytest-cov==2.8.1", "pyspark==2.4.5"]
    },
)
