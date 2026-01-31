from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Union, Optional

class PipelineBase(ABC):
    """
    Abstract base class for all ETL pipelines.
    Defines the interface for extract-transform-load steps.
    """

    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config

    @abstractmethod
    def extract(self) -> Union[DataFrame, dict]:
        """Extract data from source"""
        pass

    @abstractmethod
    def transform(self, df_or_dfs: Union[DataFrame, dict], ctx) -> DataFrame:
        """Transform the extracted data"""
        pass

    @abstractmethod
    def load(self, df):
        """Load the transformed data to sink"""
        pass

    def run(self):
        """Orchestrate ETL flow"""
        ctx = self.create_execution_context()
        self.logger.info(f"Process Context: {ctx.process}")

        load_type = getattr(self.config, "load_type", self.config.get("load_type", "full"))
        try:
            ctx.job_control.start_run(load_type)
            self.pre_etl(ctx)

            df_or_dfs = self.extract()
            df = self.transform(df_or_dfs, ctx)
            self.load(df)

            self.post_etl(ctx)
        except Exception as e:
            self.on_failure(e, ctx)
            raise
        
    
    def pre_etl(self, ctx):
        """Hook for pre-ETL operations"""
        self.logger.debug("Default pre-etl: no-op")
        
    
    def post_etl(self, ctx):
        """Hook for post-ETL operations"""
        self.logger.debug("Default post-etl: no-op")
    
    def on_failure(self, error, ctx):
        """Hook for failure handling"""
        self.logger.error(f"Pipeline failed: {error}")
        

    def create_execution_context(self):
        from core.context.process_context import create_process_context, process_context_to_string
        from core.control.job_control import JobControl
        from core.context.execution_context import ExecutionContext

        process_ctx = create_process_context(
            pipeline_version=self.config["pipeline_version"],
            is_backfill=self.config.get("is_backfill", False),
            process_id=self.config["process_id"],
            run_id=self.config.get("run_id"),    # will be overriden if orchestrator provides one
            process_name=self.config["process_name"],
            spark=self.spark,
            force_new_run_id=False,
            orchestrator_context=None
        )

        job_control = JobControl(self.spark, process_ctx)

        return ExecutionContext(
            process=process_ctx,
            job_control=job_control
        )
