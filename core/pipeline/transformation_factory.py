from core.pipeline.base import PipelineBase

class TransformationPipelineFactory:
    """
    Factory to create transformation (silver/gold) pipelines
    """

    _pipeline_registry = {
        "nse_daily_bhavcopy_silver": "transformation.silver.nse.daily_bhavcopy_silver:NSEDailyBhavcopySilver",
        "nse_historical_silver": "transformation.silver.nse.historical_silver:NSEHistoricalSilver"
    }

    @staticmethod
    def get_pipeline(pipeline_type: str, spark, logger, config) -> PipelineBase:
        if pipeline_type not in TransformationPipelineFactory._pipeline_registry:
            raise ValueError(f"Unsupported transformation pipeline type: {pipeline_type}")
        
        module_path, class_name = TransformationPipelineFactory._pipeline_registry[pipeline_type].split(":")
        module = __import__(module_path, fromlist=[class_name])
        pipeline_class = getattr(module, class_name)
        return pipeline_class(spark, logger, config)
