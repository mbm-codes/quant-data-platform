# This module implements the NSE Historical Data Pipeline
# It reads historical stock price data from CSV files,
# transforms it, enforces schema contracts, performs data quality checks,
# and loads it into an Iceberg table in a data lakehouse.
# The pipeline is configurable per environment (local, dev, prod).
# Key features include schema enforcement with strict and non-strict modes,
# metadata enrichment, and post-load cleanup of Iceberg snapshots and orphan files.
# Note - The source data is entire NSE historical data until 2023-10-31  

import os
import yaml
import copy
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType  
from lakehouse.iceberg.spark_session import SparkSessionBuilder
from core.logging.logging import QDPLogger
from core.context.process_context import create_process_context, process_context_to_string
from core.pipeline.base import PipelineBase
from core.pipeline.factory import PipelineFactory
from core.quality.checks import DataQualityChecks
from core.config.loader import load_config
from core.spark_dataframe.transforms import normalize_column_names
from core.spark_dataframe.actions import safe_count
from core.spark_dataframe.schema import enforce_schema
    
# ==========================
# Schemas and DDLs
# ==========================

# What we EXPECT to see in the CSV (strings because CSV)
RAW_INPUT_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("dividends", StringType(), True),
    StructField("stock_splits", StringType(), True),
])

EXPECTED_OUTPUT_SCHEMA = StructType([
    StructField("trade_date", DateType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume_qty", LongType(), True),
    StructField("dividends", DoubleType(), True),
    StructField("stock_splits", DoubleType(), True),
    StructField("symbol", StringType(), True),
    StructField("data_src", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingestion_ts", DateType(), True),
    StructField("execution_date", DateType(), True),
    StructField("run_id", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("process_id", StringType(), True),
    StructField("pipeline_version", StringType(), True),
    StructField("is_backfill", StringType(), True),
    StructField("trade_year", LongType(), True),

])
create_table_ddl = """
        CREATE TABLE IF NOT EXISTS local.market_lakehouse.bronze_nse_historical_prices_raw (
            symbol STRING,
            trade_date DATE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume_qty BIGINT,
            dividends DOUBLE,
            stock_splits DOUBLE,
            exchange STRING,
            data_src STRING,        
            source_file STRING,        
            ingestion_ts TIMESTAMP,
            execution_date DATE,
            run_id STRING,
            job_name STRING,
            process_id STRING,
            pipeline_version STRING,
            is_backfill BOOLEAN,
            trade_year INT
        ) USING PARQUET
        PARTITIONED BY (trade_year);
    """

# ==========================
# Historical Data Pipeline
# ==========================
class NSEHistoricalDataPipeline(PipelineBase):
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.table_name = config["output_table"]
        self.raw_schema_strict = config["schema"]["raw"]["strict"]
        self.output_schema_strict = config["schema"]["output"]["strict"]

    # --------------------------
    # ETL Steps
    # --------------------------
    def pre_etl(self, ctx):
        self.logger.info("Running pre-etl steps for NSE Historical Data Pipeline")
        #self.spark.sql("DROP TABLE IF EXISTS local.market_lakehouse.bronze_nse_historical_prices_raw")  
        #self.spark.sql(create_table_ddl)
    
    def post_etl(self, ctx):
        self.logger.info("Running post-etl steps for NSE Historical Data Pipeline")
        self._post_etl_cleanup(ctx)

    def extract(self):
        try:
            self.logger.info(f"Reading historical files from {self.config['input_pattern']}")
            df = self.spark.read.options(header=True).csv(self.config["input_pattern"])
            self.logger.debug(f"Input schema: {df.schema.simpleString()}")
          
            df = normalize_column_names(df)
            df, missing_cols_added = enforce_schema(df, RAW_INPUT_SCHEMA, logger=self.logger, strict=self.raw_schema_strict, stage="raw_input")
            # --------------------------
            # Data Quality Checks
            # --------------------------
            dq = DataQualityChecks(logger=self.logger)
            dq.assert_no_nulls(df, [c for c in df.columns if c not in missing_cols_added ])
            dq.assert_positive_values(df, self.config.get("data_quality_checks", {}).get("raw_input", {}).get("positive_values", []))
            self.logger.info(f"Data quality metrics: {dq.metrics} ")

            return df
        except Exception as e:
            self.logger.error(f"Failed to extract historical data: {e}")
            raise ValueError("Extraction failed") from e
       

    def transform(self, df_or_dfs, ctx) -> DataFrame:
        try:
            self.logger.info("Starting transformations on historical data")
            if isinstance(df_or_dfs, DataFrame):
                df = df_or_dfs
            elif isinstance(df_or_dfs, dict):
                df = next(iter(df_or_dfs.values()))
            else:
                raise ValueError("df_or_dfs must be a DataFrame or dict")

            df = self._derive_business_fields(df)
            
            df = self._add_metadata(df, ctx)

            df, _ = enforce_schema(df, EXPECTED_OUTPUT_SCHEMA, logger=self.logger, strict=self.output_schema_strict, stage="output")

           
            self.logger.debug(f"Transformed schema: {df.schema.simpleString()}")
            return df
        except Exception as e:
            self.logger.error(f"Error transforming historical data: {e}")
            raise
        

    def load(self, df):
        record_count = safe_count(df, logger=self.logger)
        self.logger.info(f"Writing {record_count:,} records to Iceberg table {self.table_name}")

        if record_count is None or record_count == 0:
            self.logger.warning("No records to write. Skipping load step.")
            return
        
        try:
            df.writeTo(self.table_name).overwrite(sf.expr("true"))
            self.logger.info(f"Successfully wrote {record_count:,} records")
        except Exception as e:
            self.logger.error(f"Error writing to Iceberg table: {e}")
            raise
    


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

    def _derive_business_fields(self, df):
        # Extract symbol from filename
        df = df.withColumn(
            "symbol",
            sf.upper(sf.regexp_extract(sf.lower(sf.input_file_name()), r"([^/]+)\.csv\.gz$", 1))
        )

        # Reorder columns
        cols = ["symbol"] + [c for c in df.columns if c != "symbol"]
        df = df.select(*cols)

        # Rename and cast columns
        df = df.withColumn("trade_date", sf.to_date(sf.col("date"))).drop("date")
        for src_col, (dst_col, dtype) in self.config["casts"].items():
            if src_col in df.columns:
                df = df.withColumn(dst_col, sf.col(src_col).cast(dtype)).drop(src_col)

        df = df.withColumn("dividends", sf.col("dividends").cast("double"))
        df = df.withColumn("stock_splits", sf.col("stock_splits").cast("double"))

        # Add static columns
        for col, val in self.config.get("static_columns", {}).items():
            df = df.withColumn(col, sf.lit(val))

        return df

    def _add_metadata(self, df, ctx):
       # Metadata enrichment
        meta_cols = {
            "source_file": sf.input_file_name(),
            "ingestion_ts": sf.lit(ctx.process_timestamp),
            "execution_date": sf.lit(ctx.process_date),
            "run_id": sf.lit(ctx.run_id),
            "job_name": sf.lit(ctx.process_name),
            "process_id": sf.lit(ctx.process_id),
            "pipeline_version": sf.lit(ctx.pipeline_version),
            "is_backfill": sf.lit(ctx.is_backfill),
            "trade_year": sf.year("trade_date")
        }
        for col, expr in meta_cols.items():
            df = df.withColumn(col, expr)

        return df
    
    
    
    def _post_etl_cleanup(self, ctx):
        try:
            self.logger.info("Running Iceberg cleanup")

            one_hour_ago = ctx.process_timestamp - timedelta(days=1)
            timestamp_str = one_hour_ago.strftime("%Y-%m-%d %H:%M:%S")

            # self.spark.table(self.table_name).remove_orphan_files() \
            # .older_than(one_hour_ago) \
            # .execute()


            self.spark.sql(f"""
                CALL local.system.expire_snapshots(
                    table => '{self.table_name}',
                    older_than => TIMESTAMP '{timestamp_str}',
                    retain_last => 1
                )
            """)

            self.spark.sql(f"""
                CALL local.system.remove_orphan_files(
                    table => '{self.table_name}',
                    older_than => TIMESTAMP '{timestamp_str}'
                )
            """)
        except Exception as e:
            self.logger.error(f"Error during post-etl cleanup: {e}")
            raise e



# ==========================
# Main entrypoint
# ==========================
def main():
    # ==========================
    # Environment setup
    # ==========================
    
    ENV = os.getenv("QDP_ENV", "local")
    conf_file = f"./config/ingestion/nse/historical/{ENV}_nse_historical_load.yaml"
    CONFIG = load_config(ENV, conf_file)
    
    # Compute log paths early
    app_log_file = CONFIG.get("app_log_file") or f"./logs/{ENV}_historical_load_app.log"
    spark_log_file = CONFIG.get("spark_log_file") or f"./logs/{ENV}_historical_load_spark.log"

    # ==========================
    # Initialize logger
    # ==========================
    logger = QDPLogger(
        name=f"{ENV}_historical_load",
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
            app_name=f"qdp-{ENV}-nse-historical-load",
            environment=ENV,
            warehouse_path=warehouse_path,
            catalog_name=catalog_name
        ).get_spark()
            
        logger.attach_spark_logs(spark)
        start_ts = datetime.now(timezone.utc)
        logger.info(f"Starting historical data load [{ENV}] at {start_ts}")

        # ==========================
        # Create pipeline and run
        # ==========================
        pipeline = PipelineFactory.get_pipeline("nse_historical", spark, logger, copy.deepcopy(CONFIG))
        pipeline.run()


        end_ts = datetime.now(timezone.utc)
        duration = (end_ts - start_ts).total_seconds()
        logger.info(f"Ending historical data load [{ENV}] at {end_ts.isoformat()} | (Duration: {duration}s)")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()