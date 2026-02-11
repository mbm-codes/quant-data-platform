from core.metrics.emitter import MetricsEmitter
from pyspark.sql import Row
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    MapType,
)



class SparkMetricsEmitter(MetricsEmitter):
    def __init__(self, spark, table_name: str, default_tags: dict):
        self.spark = spark
        self.table_name = table_name
        self.default_tags = default_tags or {}
        self.schema = StructType([
                StructField("ts", TimestampType(), False),
                StructField("metric_name", StringType(), False),
                StructField("metric_type", StringType(), False),
                StructField("metric_value", DoubleType(), False),
                StructField("pipeline", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("tags", MapType(StringType(), StringType()), True),
            ]       
        )


    def _emit(self, name, metric_type, value, tags):

        merged_tags = {**self.default_tags, **(tags or {})}

        # Extract reserved dimensions
        pipeline = merged_tags.pop("pipeline", None)
        table_name = merged_tags.pop("table", None)
        layer = merged_tags.pop("layer", None)

        row = Row(
            ts=datetime.now(timezone.utc),
            metric_name=name,
            metric_type=metric_type,
            metric_value=float(value),
            pipeline=pipeline,
            table_name=table_name,
            layer=layer,
            tags=merged_tags
        )

        self.spark.createDataFrame([row], schema=self.schema) \
            .writeTo(self.table_name) \
            .append()


    def increment(self, name, value=1, tags=None):
        self._emit(name, "counter", value, tags)

    def gauge(self, name, value, tags=None):
        self._emit(name, "gauge", value, tags)

    def timing(self, name, value_ms, tags=None):
        self._emit(name, "timing", value_ms, tags)
