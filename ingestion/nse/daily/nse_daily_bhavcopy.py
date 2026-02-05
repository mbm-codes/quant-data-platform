import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import HTTPError
import os
from datetime import datetime, timedelta, timezone
import gzip
import time
from lakehouse.iceberg.spark_session import SparkSessionBuilder
from core.pipeline.base import PipelineBase
from core.pipeline.factory import PipelineFactory
from core.config.loader import load_config
from core.logging.logging import QDPLogger
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, DateType, DoubleType, LongType
from core.spark_dataframe.transforms import normalize_column_names, trim_and_nullify_strings, standardize_date, cast_and_rename_columns
import copy
import random
import argparse
from core.quality.checks import DataQualityChecks
from core.spark_dataframe.actions import safe_count
from core.spark_dataframe.schema import enforce_schema
from core.context.process_context import process_context_to_string
from core.quality.factory.file_checks import build_file_checks
from core.quality.factory.column_checks import build_column_checks
from core.quality.factory.table_checks import build_table_checks
from core.quality.runner import DataQualityRunner

# What we EXPECT to see in the CSV (strings because CSV)
RAW_INPUT_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("series", StringType(), True),	
    StructField("date1", StringType(), True),
    StructField("prev_close", StringType(), True),
    StructField("open_price", StringType(), True),
    StructField("high_price", StringType(), True),
    StructField("low_price", StringType(), True),
    StructField("last_price", StringType(), True),
    StructField("close_price", StringType(), True),
    StructField("avg_price", StringType(), True),
    StructField("ttl_trd_qnty", StringType(), True),
    StructField("turnover_lacs", StringType(), True),
    StructField("no_of_trades", StringType(), True),
    StructField("deliv_qty", StringType(), True),
    StructField("deliv_per", StringType(), True),
])

EXPECTED_OUTPUT_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("series", StringType(), True),	
    StructField("trade_date", DateType(), True),
    StructField("prev_close", DoubleType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("ttl_trd_qnty", LongType(), True),
    StructField("turnover_lacs", DoubleType(), True),
    StructField("no_of_trades", LongType(), True),
    StructField("deliv_qty", LongType(), True),
    StructField("deliv_per", DoubleType(), True),

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



class NSEDailyData(PipelineBase):
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.table_name = config["output_table"]
        self.raw_schema_strict = config["schema"]["raw"]["strict"]
        self.output_schema_strict = config["schema"]["output"]["strict"]

    
    def extract(self):
        self.logger.info("Starting fetching NSE daily bhavcopy!")
        base_url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_"
        output_path = "data_sources/nse/nse_bhavcopy_from_nov_2023/"
        date_list = get_dates()
        try:
            for dt in date_list:
                #fetch_data(base_url+dt+".csv", output_path, self.logger)
                pass
        except Exception as e:
            self.logger.error(f"Error encountered : {str(e)}")
            raise
        #read_data
        self.logger.info("Ended fetching NSE daily bhavcopy!")
        try:
            dq_cfg = self.config.get("data_quality", None)

            fc_dq_checks = {}
            if dq_cfg:
                fc_dq_checks = dq_cfg.get("file_checks", [])

            file_checks = build_file_checks(self.spark, fc_dq_checks, self.config["input_pattern"])
            runner = DataQualityRunner(file_checks, self.logger)
            results = runner.run()

            for r in results:
                if r.status.value == "FAIL":
                    raise RuntimeError(f"DQ failed: {r.check_name} - {r.message}")


            self.logger.info(f"Reading historical files from {self.config['input_pattern']}")
            df = self.spark.read.options(header=True).csv(self.config["input_pattern"])

            df = normalize_column_names(df)
            df = trim_and_nullify_strings(df)
            df, _ = enforce_schema(df, RAW_INPUT_SCHEMA, logger=self.logger, strict=self.raw_schema_strict, stage="raw_input")


            self.logger.info(f"Input schema: {df.schema.simpleString()}")

            return df
        except Exception as e:
            self.logger.error(f"Failed to extract NSE Daily Bhavcopy data: {e}")
            raise ValueError("Extraction failed") from e

    
    def pre_etl(self, ctx):
        self.logger.info("Running pre-etl steps for NSE Historical Data Pipeline")
        #self.spark.sql("DROP TABLE IF EXISTS local.market_lakehouse.bronze_nse_historical_prices_raw")  
        #self.spark.sql(create_table_ddl)
    
    def post_etl(self, ctx):
        self.logger.info("Running post-etl steps for NSE Historical Data Pipeline")
    
    def transform(self, df_or_dfs, ctx):
        try:
            if isinstance(df_or_dfs, DataFrame):
                df = df_or_dfs
            elif isinstance(df_or_dfs, dict):
                df = next(iter(df_or_dfs.values()))
            else:
                raise ValueError("df_or_dfs must be a DataFrame or dict")

            dq_cfg = self.config.get("data_quality", None)

            col_dq_checks = []
            if dq_cfg:
                col_dq_checks = dq_cfg.get("column_checks", [])

            col_checks = []
            col_checks = build_column_checks(df, col_dq_checks)
            runner = DataQualityRunner(col_checks, self.logger)
            results = runner.run()            


            df = standardize_date(df, ["date1"], "dd-MMM-yyyy")
            df = self._add_metadata(df, ctx)
            
            # Add static columns
            for col, val in self.config.get("static_columns", {}).items():
                df = df.withColumn(col, sf.lit(val))

            df = cast_and_rename_columns(df, self.config["casts"])
            
            tbl_dq_checks = []
            if dq_cfg:
                tbl_dq_checks = dq_cfg.get("table_checks", [])
            
            tbl_checks = []
            tbl_checks = build_table_checks(df, tbl_dq_checks)
            runner = DataQualityRunner(tbl_checks, self.logger)
            results = runner.run()
            
            df, _ = enforce_schema(df, EXPECTED_OUTPUT_SCHEMA, logger=self.logger, strict=self.output_schema_strict, stage="output")
            self.logger.debug(f"Transformed schema: {df.schema.simpleString()}")
            return df
        except Exception as e:
            self.logger.error(f"Error transforming historical data: {e}")
            raise

    def _add_metadata(self, df, ctx):
       # Metadata enrichment
        meta_cols = {
            "source_file": sf.input_file_name(),
            "ingestion_ts": sf.lit(ctx.process.process_timestamp),
            "execution_date": sf.lit(ctx.process.process_date),
            "run_id": sf.lit(ctx.process.run_id),
            "job_name": sf.lit(ctx.process.process_name),
            "process_id": sf.lit(ctx.process.process_id),
            "pipeline_version": sf.lit(ctx.process.pipeline_version),
            "is_backfill": sf.lit(ctx.process.is_backfill),
            "trade_year": sf.year("date1")
        }
        for col, expr in meta_cols.items():
            df = df.withColumn(col, expr)

        return df
    
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
    
        ctx = self.create_execution_context()
    
        self.logger.info(f"Execution Context: {process_context_to_string(ctx.process)}")

        load_type = self.config.get("load_type", "full")
        try:
            ctx.job_control.start_run(load_type=load_type)
            self.logger.info(f"Job started with run_id: {ctx.process.run_id}")
            self.pre_etl(ctx)
            df = self.extract()
            df = self.transform(df, ctx)
            self.load(df)
            self.post_etl(ctx)

            # Mark success

            max_ts = df.agg({"trade_date": "max"}).collect()[0][0] if isinstance(df, DataFrame) else None
            rows_written = df.count() if isinstance(df, DataFrame) else None
            ctx.job_control.mark_success(max_ts, rows_written)

            self.logger.info(f"Pipeline completed successfully. Rows written: {rows_written} ")
        except Exception as e:
            # Mark failure

            ctx.job_control.mark_failure(str(e))
            self.logger.error(f"Pipeline failed: {e}")
            raise


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

def get_dates():
    current_date = datetime.now().date()
    end_date = current_date - timedelta(days=1)
    start_date = datetime.strptime("2024-03-30", "%Y-%m-%d").date()
    list_of_dates = []
    while start_date <= end_date:
        list_of_dates.append(start_date.strftime('%d%m%Y'))
        start_date = start_date + timedelta(days=1)

    return list_of_dates

def fetch_data(url, output_path, logger):
    
    try:
        session = requests.Session()
        retries = Retry(total=4, backoff_factor=1,
                        status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(url, headers=headers, timeout=30)
        output_file_name = os.path.basename(url)
        if response.status_code == 404:
            logger.warning(f"File not found, {output_file_name}")
        else:
            response.raise_for_status()
            #output_path = os.path.basename(url)
            with gzip.GzipFile(output_path+ output_file_name + ".gz", "wb") as f:
                f.write(response.content)
        
            logger.info(f"Downloaded successfully, {output_file_name}")
        time.sleep(random.randint(2,4))
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error encountered: {str(e)}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Error encountered: {str(e)}")

def parse_args():
    parser = argparse.ArgumentParser(description="NSE Daily Bhavcopy Pipeline")


    parser.add_argument(
        "--env",
        default=os.getenv("QDP_ENV", "local"),
        help="Execution environment (local/dev/prod)"

    )

    parser.add_argument(
        "--is-backfill",
        action="store_true",
        help="Run pipeline in backfill mode"

    )

    parser.add_argument(
        "--run-id",
        help="Override run_id (otherwise auto-generated)"

    )

    parser.add_argument(
        "--process-id",
        help="Override process_id"

    )

    parser.add_argument(
        "--input-pattern",
        help="Override input file pattern"

    )

    parser.add_argument(
        "--execution-date",
        help="Execution date (YYYY-MM-DD), useful for backfills"

    )

    return parser.parse_args()

    
    
if __name__ == "__main__":

    # ==========================
    # Environment setup
    # ==========================
    args = parse_args()

    ENV = os.getenv("QDP_ENV", "local")
    conf_file = f"./config/ingestion/nse/daily/{ENV}_nse_daily_bhavcopy_load.yaml"
    CONFIG = load_config(ENV, conf_file)

    # Apply command line overrides
    for attr in ["run_id", "process_id", "input_pattern", "execution_date", "is_backfill"]:
        val = getattr(args, attr, None)
        if val is not None:
            CONFIG[attr] = val
    
    # Compute log paths early
    app_log_file = CONFIG.get("app_log_file") or f"./logs/{ENV}_nse_daily_bhavcopy_load_app.log"
    spark_log_file = CONFIG.get("spark_log_file") or f"./logs/{ENV}_nse_daily_bhavcopy_load_spark.log"

    # ==========================
    # Initialize logger
    # ==========================
    logger = QDPLogger(
        name=f"{ENV}_nse_daily_bhavcopy_load",
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
            app_name=f"qdp-{ENV}-nse-daily-bhavcopy-load",
            environment=ENV,
            warehouse_path=warehouse_path,
            catalog_name=catalog_name
        ).get_spark()
            
        logger.attach_spark_logs(spark)

        pipeline = PipelineFactory.get_pipeline("nse_daily_bhavcopy_load", spark, logger, copy.deepcopy(CONFIG))
        pipeline.run()


    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
    



