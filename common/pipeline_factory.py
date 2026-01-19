from common.pipeline_base import PipelineBase

class PipelineFactory:
    """
    Factory class to create ETL pipelines based on source type.
    """

    @staticmethod
    def get_pipeline(pipeline_type: str, spark, logger, config) -> PipelineBase:
        if pipeline_type == "nse_historical":
            from ingestion.nse.historical.historical_loads import NSEHistoricalDataPipeline
            return NSEHistoricalDataPipeline(spark, logger, config)
        else:
            raise ValueError(f"Unsupported pipeline type: {pipeline_type}")
