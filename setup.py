from setuptools import find_packages, setup

setup(
    name="soda-spark",
    packages=find_packages("src/"),
    version="0.1.0.dev",
    description="Soda SQL API for PySpark data frame",
    author="Soda",
    install_requires=[
        "soda-sql-spark>=2.0.0,<3.0.0",
        "pyspark>=3.0.0,<4.0.0",
    ],
    extras_require={
        "dev": [
            "pre-commit==2.14.1",
            "pytest-spark==0.6.0",
            "pytest-cov==2.12.1",
        ]
    },
    package_dir={"": "src"},
)
