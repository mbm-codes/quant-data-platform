import os
import yaml
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType  
from lakehouse.iceberg.spark_session import SparkSessionBuilder
from common.logging.logging import QDPLogger
from ingestion.common.processing_metadata import create_process_context, process_context_to_string
from common.pipeline_base import PipelineBase
from common.pipeline_factory import PipelineFactory

# ==========================
# Load configuration per environment
# ==========================
ENV = os.getenv("QDP_ENV", "local")  # local/dev/prod
CONFIG_FILE = f"./config/ingestion/nse/historical/{ENV}_nse_historical_load.yaml"

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

with open(CONFIG_FILE, "r") as f:
    CONFIG = yaml.safe_load(f)

RAW_SCHEMA_STRICT = CONFIG["schema"]["raw"]["strict"]
OUTPUT_SCHEMA_STRICT = CONFIG["schema"]["output"]["strict"]


def normalize_column_names(df):
    return df.select([sf.col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

    
# ==========================
# Historical Data Pipeline
# ==========================
class NSEHistoricalDataPipeline(PipelineBase):
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.table_name = config["output_table"]

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
            df = self.enforce_schema(df, RAW_INPUT_SCHEMA, logger=self.logger, strict=RAW_SCHEMA_STRICT, stage="raw_input")
            return df
        except Exception as e:
            self.logger.error(f"Error reading historical files: {e}")
            raise e
       

    def transform(self, df, ctx):
        try:
            self.logger.info("Starting transformations on historical data")

            df = self._derive_business_fields(df)
            
            df = self._add_metadata(df, ctx)

            df = self.enforce_schema(df, EXPECTED_OUTPUT_SCHEMA, logger=self.logger, strict=OUTPUT_SCHEMA_STRICT, stage="output")

            self.logger.debug(f"Transformed schema: {df.schema.simpleString()}")

        except Exception as e:
            self.logger.error(f"Error transforming historical data: {e}")
            raise
        return df

    def load(self, df):
       
        

        try:
            df.writeTo(self.table_name).overwrite(sf.expr("true"))
            record_count = df.count()
            self.logger.info(f"Writing {record_count:,} records to Iceberg table {self.table_name}")
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
        df = df.withColumn("source_file", sf.input_file_name())
        df = df.withColumn("ingestion_ts", sf.lit(ctx.process_timestamp))
        df = df.withColumn("execution_date", sf.lit(ctx.process_date))
        df = df.withColumn("run_id", sf.lit(ctx.run_id))
        df = df.withColumn("job_name", sf.lit(ctx.process_name))
        df = df.withColumn("process_id", sf.lit(ctx.process_id))
        df = df.withColumn("pipeline_version", sf.lit(ctx.pipeline_version))
        df = df.withColumn("is_backfill", sf.lit(ctx.is_backfill))
        df = df.withColumn("trade_year", sf.year("trade_date"))
        return df
    
    def enforce_schema(self, df, schema, logger=None, strict=True, stage="unknown"):
        """
        Enforces a schema contract.

        strict=True:
        - missing columns -> error
        - extra columns -> error

        strict=False:
        - missing columns -> add as null
        - extra columns -> drop
        """

        expected_cols = {field.name for field in schema}
        actual_cols = set(df.columns)
        extra_cols = actual_cols - expected_cols
        missing_cols = expected_cols - actual_cols

        if strict:
            errors = []
            if missing_cols:
                errors.append(f"[{stage}] Missing columns: {sorted(missing_cols)}")
            if extra_cols:
                errors.append(f"[{stage}] Extra columns: {sorted(extra_cols)}")
            if errors:
                msg = f"[{stage}] Schema validation failed: " + "; ".join(errors)
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            # Column order enforcement
            return df.select([f.name for f in schema])
        # ---------------------------
        # STRICT MODE: FALSE
        # ---------------------------

        for field in missing_cols:
            df = df.withColumn(field, sf.lit(None).cast(schema[field].dataType))
            if logger:
                logger.warning(f"[{stage}] Missing column added as null: {field}")
        
        if extra_cols:
            if logger:
                logger.warning(f"[{stage}] Extra columns dropped: {sorted(extra_cols)}")
            df = df.drop(*extra_cols)
        
        return df.select([f.name for f in schema])
    
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
                    table => 'market_lakehouse.intgr_bronze_nse_historical_prices_raw',
                    older_than => TIMESTAMP '{timestamp_str}',
                    retain_last => 1
                )
            """)

            self.spark.sql(f"""
                CALL local.system.remove_orphan_files(
                    table => 'market_lakehouse.intgr_bronze_nse_historical_prices_raw',
                    older_than => TIMESTAMP '{timestamp_str}'
                )
            """)
        except Exception as e:
            self.logger.error(f"Error during post-etl cleanup: {e}")
            raise



# ==========================
# Main entrypoint
# ==========================
if __name__ == "__main__":
    # Initialize logger
    logger = QDPLogger(
        name=f"{ENV}_historical_load",
        app_log_file=CONFIG.get("app_log_file"),
        spark_log_file=CONFIG.get("spark_log_file"),
        level=QDPLogger.INFO
    )
    try:
        spark = SparkSessionBuilder(
                    app_name="qdp-local",
                    environment="local",
                ).get_spark()

        # Environment-aware log paths
        app_log_file = CONFIG.get("app_log_file") or f"./logs/{ENV}_historical_load_app.log"
        spark_log_file = CONFIG.get("spark_log_file") or f"./logs/{ENV}_historical_load_spark.log"

        
        logger.attach_spark_logs(spark)

        logger.info(f"Starting historical data load [{ENV}] at {datetime.now(timezone.utc)}")

        pipeline = PipelineFactory.get_pipeline("nse_historical", spark, logger, CONFIG)
        pipeline.run()

        logger.info(f"Ending historical data load [{ENV}] at {datetime.now(timezone.utc)}")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        spark.stop()