import os
from pyspark.sql import SparkSession


# macOS: Force PySpark to use the correct Java 11
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
# os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")

#Use of simple factory, singleton design pattern

# TBD : Use this config class to have all configs in one place 
# class SparkSessionConfig:
    
#     def __init__(self, catalog_name, warehouse_path ):
#         self.configs = {
#             "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
#             "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#             f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
#             f"spark.sql.catalog.{catalog_name}.type": "hadoop",
#             f"spark.sql.catalog.{catalog_name}.warehouse": warehouse_path,
#             "spark.driver.extraJavaOptions": "-Djava.security.manager=allow",
#             "spark.executor.extraJavaOptions": "-Djava.security.manager=allow"
#         }

class SparkSessionBuilder:
    """
    Factory class to create and configure SparkSession with Iceberg support.

    Environment-aware:
        - local
        - dev
        - test
        - prod
    """
    _spark_instance = None
    def __init__(self, app_name="qdp-spark-session", warehouse_path="./warehouse", catalog_name="local", environment="prod"):
        self.app_name = app_name
        self.warehouse_path = os.path.abspath(warehouse_path)
        self.catalog_name = catalog_name
        self.environment = environment

        if warehouse_path:
            self.warehouse_path = os.path.abspath(warehouse_path)
        else:
            self.warehouse_path = self._default_warehouse_path()

        os.makedirs(self.warehouse_path, exist_ok=True)
    
    def get_spark(self):

        if SparkSessionBuilder._spark_instance is not None:
            return SparkSessionBuilder._spark_instance
        #import os
        #os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions='-Xmx1g -XX:+PrintGCDetails -XX:+PrintGCDateStamps' pyspark-shell"
        
        builder = SparkSession.builder.appName(self.app_name)

        # ------------------------------------------------------------------
        # Core Iceberg configuration (same everywhere)
        # ------------------------------------------------------------------
        builder = (
            builder
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{self.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.{self.catalog_name}.type",
                "hadoop",
            )
            .config(
                f"spark.sql.catalog.{self.catalog_name}.warehouse",
                self.warehouse_path,
            )
            .config("spark.app.env", self.environment)
        )

        # ------------------------------------------------------------------
        # Environment-specific tuning
        # ------------------------------------------------------------------
        builder = self._apply_environment_configs(builder)

        SparkSessionBuilder._spark_instance = builder.getOrCreate()
        return SparkSessionBuilder._spark_instance

    def _default_warehouse_path(self):
        return {
             "local": "./warehouse",
            #  "test": "./warehouse",
            #  "dev": "/mnt/dev/warehouse",
            #  "prod": "/mnt/prod/warehouse"
        }.get(self.environment, "./warehouse")

    def _apply_environment_configs(self, builder: SparkSession.Builder) -> SparkSession.Builder:
        """
        Apply environment-specific Spark configurations.
        """

        # -------------------------------
        # DEV
        # -------------------------------
        if self.environment == "dev":
            return (
                builder
                .config("spark.ui.enabled", "true")
                .config("spark.driver.memory", "6g")
                .config("spark.executor.memory", "6g")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.iceberg.write.target-file-size-bytes", 256 * 1024 * 1024)
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "file:///tmp/spark-events")
            )
        elif self.environment == "test":
            return (
                builder
                .master("local[2]")
                .config("spark.ui.enabled", "true")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "10")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.iceberg.write.target-file-size-bytes", 50 * 1024 * 1024)
                .config("spark.eventLog.enabled", "false")
            )
        elif self.environment == "local":
            return (
                builder
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.shuffle.partitions", "5")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.iceberg.write.target-file-size-bytes", 20 * 1024 * 1024)
                .config("spark.eventLog.enabled", "false")
            )

        # -------------------------------
        # PROD (default)
        # -------------------------------
        return (
            builder
            .config("spark.ui.enabled", "true")
            .config("spark.driver.memory", "16g")
            .config("spark.executor.memory", "16g")
            .config("spark.sql.shuffle.partitions", "1000")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.iceberg.write.target-file-size-bytes", 512 * 1024 * 1024)
            .config("spark.eventLog.enabled", "true")
        )

        


        
