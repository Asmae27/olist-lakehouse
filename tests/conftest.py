import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale, sans MinIO ni Delta : on ne teste que la logique des DataFrames."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("olist-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
