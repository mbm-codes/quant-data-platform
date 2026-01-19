import pytest
from lakehouse.iceberg.spark_session import SparkSessionBuilder

warehouse_path = "./warehouse"
catalog_name = "local"

@pytest.fixture(scope="session")
def spark_session():
    """
    Pytest fixture to create a SparkSession for testing.
    """
    spark = SparkSessionBuilder(
        app_name="qdp-test-session",
        environment="local",
    ).get_spark()
    yield spark
    spark.stop()