import os
from pyspark.sql import SparkSession


# macOS: Force PySpark to use the correct Java 11
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
# os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")

#Use of simple factory, singleton design pattern

# TBD : Use this config class to have all configs in one place 
class SparkSessionConfig:
    
    def __init__(self, catalog_name, warehouse_path ):
        self.configs = {
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": "hadoop",
            f"spark.sql.catalog.{catalog_name}.warehouse": warehouse_path,
            "spark.driver.extraJavaOptions": "-Djava.security.manager=allow",
            "spark.executor.extraJavaOptions": "-Djava.security.manager=allow"
        }

class SparkSessionBuilder:
    _spark_instance = None
    def __init__(self, app_name="qdp-spark-session", warehouse_path="./warehouse", catalog_name="local"):
        self.app_name = app_name
        self.warehouse_path = os.path.abspath(warehouse_path)
        self.catalog_name = catalog_name

        os.makedirs(self.warehouse_path, exist_ok=True)
    
    def get_spark(self):
        #import os
        #os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions='-Xmx1g -XX:+PrintGCDetails -XX:+PrintGCDateStamps' pyspark-shell"

        if SparkSessionBuilder._spark_instance is None:
            try:

                self._spark_instance = (
                    SparkSession.builder
                    .appName(self.app_name)
                    .config(
                        "spark.jars.packages",
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
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
                    # Required for Java 17+
                    # .config(
                    #     "spark.driver.extraJavaOptions",
                    #     "-Djava.security.manager=allow"
                    # )
                    # .config(
                    #     "spark.executor.extraJavaOptions",
                    #     "-Djava.security.manager=allow"
                    # )
                    .config("spark.driver.memory", "6g")
                    .config("spark.executor.memory", "6g")
                    .config("spark.sql.shuffle.partitions", "200")
                    .config("spark.sql.parquet.block.size", 64 * 1024 * 1024)
                    .config("spark.sql.parquet.compression.codec", "snappy")
                    .config("spark.app.env", "local")
                    .config("spark.sql.iceberg.write.target-file-size-bytes", 256 * 1024 * 1024)
                    .config("spark.sql.adaptive.enabled", True)
                    .config("spark.sql.adaptive.coalescePartitions.enabled", True)

                    # .config("spark.eventLog.enabled", "true")
                    # .config("spark.eventLog.dir", "file:///tmp/spark-events")

                    .getOrCreate()
                )
            except Exception as e:
                print(f"Error creating SparkSession: {str(e)}")
                raise e

        
        return self._spark_instance
