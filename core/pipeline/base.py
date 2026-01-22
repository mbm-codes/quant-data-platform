from abc import ABC, abstractmethod

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
    def extract(self):
        """Extract data from source"""
        pass

    @abstractmethod
    def transform(self, df, ctx):
        """Transform the extracted data"""
        pass

    @abstractmethod
    def load(self, df):
        """Load the transformed data to sink"""
        pass

    def run(self):
        """Orchestrate ETL flow"""
        ctx = self.create_process_context()
        self.logger.info(f"Process Context: {ctx}")
        try:
            self.pre_etl(ctx)

            df = self.extract()
            df = self.transform(df, ctx)
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
        

    def create_process_context(self):
        from ingestion.common.processing_metadata import create_process_context, process_context_to_string
        ctx = create_process_context(
            pipeline_version=self.config["pipeline_version"],
            is_backfill=False,
            process_id=self.config["process_id"],
            run_id=self.config["run_id"],
            process_name=self.config["process_name"],
            spark=self.spark,
            force_new_run_id=False,
            orchestrator_context=None
        )
        return ctx
