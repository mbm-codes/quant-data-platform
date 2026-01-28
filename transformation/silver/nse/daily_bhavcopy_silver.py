from core.pipeline.transformation_base import SilverPipelineBase
from core.pipeline.transformation_factory import TransformationPipelineFactory
from pyspark.sql import DataFrame, functions as sf
from typing import Union
from core.spark_dataframe.actions import safe_count
from core.metadata.processing_metadata import create_process_context, process_context_to_string
from core.config.loader import load_config
from core.logging.logging import QDPLogger
from lakehouse.iceberg.spark_session import SparkSessionBuilder
from datetime import datetime, timezone
import os
import copy
from pyspark.sql.window import Window


class NSEDailyBhavcopySilver(SilverPipelineBase):
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.input_table_names = config["input_tables"]
        self.output_table_name = config["output_table"]

    def pre_etl(self, ctx):
        self.logger.info("Running pre-etl steps for NSE Daily Bhavcopy Data Pipeline")
  
    
    def post_etl(self, ctx):
        self.logger.info("Running post-etl steps for NSE Daily Bhavcopy Data Pipeline")
        

    def extract(self) -> Union[DataFrame, dict]:
        try:
        
            self.logger.info(f"Reading {''.join(self.config['input_tables'].values())} bronze layer tables")
            df_or_dfs = self.read_bronze()
            
            return df_or_dfs

        except Exception as e:
            self.logger.error(f"Failed to read {''.join(self.config['input_tables'].values())} bronze layer tables")
            raise ValueError("Failed to read") from e

    def transform(self, df_or_dfs, ctx) -> DataFrame:
        try:
            self.logger.info(f"Starting transformations on {''.join(self.config['input_tables'].values())}")
            if isinstance(df_or_dfs, DataFrame):
                df = df_or_dfs
            elif isinstance(df_or_dfs, dict):
                df = next(iter(df_or_dfs.values()))
            else:
                raise ValueError("df_or_dfs must be a DataFrame or dict")
            
          

            df = df.drop("source_file","ingestion_ts", "execution_date", "run_id", "job_name", "process_id", "pipeline_version", "is_backfill", "trade_year")
            df = self.apply_business_rules(df)
            df = self._add_metadata(df, ctx)

        
            self.logger.debug(f"Transformed schema: {df.schema.simpleString()}")
            return df
        except Exception as e:
            self.logger.error(f"Error transforming NSE Bhavcopy bronze layer -> silver layer: {e}")
            raise
    
    
    def load(self, df):

        record_count = safe_count(df, logger=self.logger)
        self.logger.info(f"Writing {record_count:,} records to Iceberg table {self.output_table_name}")

        if record_count is None or record_count == 0:
            self.logger.warning("No records to write. Skipping load step.")
            return

        try:
            df.writeTo(self.output_table_name).overwrite(sf.expr("true"))
            self.logger.info(f"Successfully wrote {record_count:,} records")
        except Exception as e:
            self.logger.error(f"Error writing to Iceberg table: {e}")
            raise
    
    def read_bronze(self) -> Union[DataFrame, dict]:
        df_map = {}
        for tbl_typ, tbl_name in self.config["input_tables"].items():
            df_map[tbl_typ] = self.spark.read.table(tbl_name)

            
        if len(df_map) == 1:
            return next(iter(df_map.values()))
        return df_map
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        # Business rules - 
        # 1 filter 
        # 2 do a dedup on entire dataset
        self.logger.info(f"Record count before deduping, {safe_count(df)}")
        #rej_records = df.
        

        df_clean = df.dropDuplicates()
        self.logger.info(f"Record count after deduping, {safe_count(df_clean)}")
       
        return df_clean

    def _add_metadata(self, df, ctx):
        # Metadata enrichment
        meta_cols = {
            "ingestion_ts": sf.lit(ctx.process_timestamp),
            "execution_date": sf.lit(ctx.process_date),
            "run_id": sf.lit(ctx.run_id),
            "job_name": sf.lit(ctx.process_name),
            "process_id": sf.lit(ctx.process_id),
            "pipeline_version": sf.lit(ctx.pipeline_version),
            "trade_year": sf.year("trade_date")
        }
        for col, expr in meta_cols.items():
            df = df.withColumn(col, expr)

        return df

    
    def write_silver(self, df: DataFrame):
        return super().write_silver(df)
    
    def run_quality_checks(self, df: DataFrame, ctx):
        return super().run_quality_checks(df, ctx)

    def run(self):
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
        self.logger.info(f"Process Context: {process_context_to_string(ctx)}")
        self.pre_etl(ctx)
        df = self.extract()
        df = self.transform(df, ctx)
        self.load(df)
        self.post_etl(ctx)

def main():
    # ==========================
    # Environment setup
    # ==========================

    ENV = os.getenv("QDP_ENV", "local")
    conf_file = f"./config/transformation/{ENV}_slvr_nse_bhavcopy_daily.yaml"
    CONFIG = load_config(ENV, conf_file)


    # Compute log paths early
    app_log_file = CONFIG.get("app_log_file") or f"./logs/{ENV}_slvr_nse_bhavcopy_daily_app.log"
    spark_log_file = CONFIG.get("spark_log_file") or f"./logs/{ENV}_slvr_nse_bhavcopy_daily_spark.log"

        # ==========================
    # Initialize logger
    # ==========================
    logger = QDPLogger(
        name=f"{ENV}_slvr_nse_bhavcopy_daily",
        app_log_file=app_log_file,
        spark_log_file=spark_log_file,
        level=QDPLogger.INFO
    )

    spark = None

    try:
        # ==========================
        # Initialize Spark Session
        # ==========================
        warehouse_path = CONFIG.get("warehouse_path", "./warehouse")
        catalog_name = CONFIG.get("catalog_name", "local")

        spark = SparkSessionBuilder(
            app_name=f"qdp-{ENV}-slvr-nse-bhavcopy-daily",
            environment=ENV,
            warehouse_path=warehouse_path,
            catalog_name=catalog_name
        ).get_spark()

        logger.attach_spark_logs(spark)
        start_ts = datetime.now(timezone.utc)
        logger.info(f"Starting Silver NSE Bhavcopy Daily transformation in [{ENV}] at {start_ts}")

        # ==========================
        # Create pipeline and run
        # ==========================
        pipeline = TransformationPipelineFactory.get_pipeline("nse_daily_bhavcopy_silver", spark, logger, copy.deepcopy(CONFIG))
        pipeline.run()

        end_ts = datetime.now(timezone.utc)
        duration = (end_ts - start_ts).total_seconds()
        logger.info(f"Ending transformation of NSE Bhavcopy Daily transformation in [{ENV}] at {end_ts.isoformat()} | (Duration: {duration}s)")
    
    except Exception as e:
        logger.error(f"Transformation execution failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")



if __name__ == "__main__":
    main()
    
