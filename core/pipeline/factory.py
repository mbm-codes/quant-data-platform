from core.pipeline.base import PipelineBase
from core.metrics.spark import SparkMetricsEmitter
import os

class PipelineFactory:
    """
    Factory class to create ETL pipelines based on source type.
    """

    @staticmethod
    def get_pipeline(pipeline_type: str, spark, logger, config) -> PipelineBase:
        ENV = os.getenv("QDP_ENV", "local")
        metrics = SparkMetricsEmitter(
                spark,
                table_name="local.control_db.metrics_events",
                default_tags={
                    "env": ENV,
                    "pipeline": pipeline_type
                }
        )
        
        if pipeline_type == "nse_historical":
            from ingestion.nse.historical.historical_loads import NSEHistoricalDataPipeline
            return NSEHistoricalDataPipeline(spark, logger, config, metrics)
        elif pipeline_type == "nse_daily_bhavcopy_load":
            from ingestion.nse.daily.nse_daily_bhavcopy import NSEDailyData
            return NSEDailyData(spark, logger, config, metrics)
        else:
            raise ValueError(f"Unsupported pipeline type: {pipeline_type}")
