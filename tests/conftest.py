import pytest

warehouse_path = "./warehouse"
catalog_name = "local"

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture to create a SparkSession for testing.
    """

    pytest.importorskip(
        "pyspark",
        reason="Legacy Spark tests require the optional PySpark environment.",
    )

    from lakehouse.iceberg.spark_session import SparkSessionBuilder
    
    spark = SparkSessionBuilder(
        app_name="qdp-test-session",
        environment="local",
    ).get_spark()
    yield spark
    spark.stop()