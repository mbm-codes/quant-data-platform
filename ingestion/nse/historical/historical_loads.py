# This module implements the NSE Historical Data Pipeline
# It reads historical stock price data from CSV files,
# transforms it, enforces schema contracts, performs data quality checks,
# and loads it into an Iceberg table in a data lakehouse.
# The pipeline is configurable per environment (local, dev, prod).
# Key features include schema enforcement with strict and non-strict modes,
# metadata enrichment, and post-load cleanup of Iceberg snapshots and orphan files.
# Note - The source data is entire NSE historical data until 2023-10-31  

import os
from time import time
import yaml
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType  
from lakehouse.iceberg.spark_session import SparkSessionBuilder
from core.logging.logging import QDPLogger
from core.context.process_context import process_context_to_string
from core.pipeline.base import PipelineBase
from core.pipeline.factory import PipelineFactory
from core.quality.checks import DataQualityChecks
from core.config.loader import load_config
from core.spark_dataframe.transforms import normalize_column_names, trim_and_nullify_strings
from core.spark_dataframe.actions import safe_count
from core.spark_dataframe.schema import enforce_schema
from core.quality.runner import DataQualityRunner
from core.quality.factory.file_checks import build_file_checks
from core.quality.factory.column_checks import build_column_checks
from core.quality.factory.table_checks import build_table_checks
from core.quality.actions import apply_actions
from core.exceptions.exceptions import DQExecutionException, PipelineException, TransformationException
from core.exceptions.errors import InputDataError, DataQualityError, SchemaEnforcementError, PipelineError, LoadError, PostETLError

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
            open_price DECIMAL(20,8),
            high_price DECIMAL(20,8),
            low_price DECIMAL(20,8),
            close_price DECIMAL(20,8),
            volume_qty BIGINT,
            dividends DECIMAL(20,8),
            stock_splits DECIMAL(20,8),
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

pipeline_name = "nse_historical"
layer = "bronze"
# ==========================
# Historical Data Pipeline
# ==========================
class NSEHistoricalDataPipeline(PipelineBase):
    def __init__(self, spark, logger, config, metrics):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.metrics = metrics
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
            start_ts = time()
        
            dq_cfg = self.config.get("data_quality") or {}
            file_checks_cfg = dq_cfg.get("file_checks", [])

            file_checks = build_file_checks(
                self.spark,
                file_checks_cfg,
                self.config["input_pattern"]
            )

            try:
                runner = DataQualityRunner(file_checks, self.logger)
                results = runner.run()
            except Exception as e:
                raise DQExecutionException("File-level DQ execution failed") from e

            failed = [r for r in results if r.status.value == "FAIL"]
            if failed:
                raise DataQualityError(
                    "File-level data quality checks failed",
                    failed_checks=failed
                )

            df = self.spark.read.options(header=True).csv(self.config["input_pattern"])
            row_count = safe_count(df, logger=self.logger)

            self.metrics.gauge(
                "pipeline.extract.rows",
                row_count,
                tags={
                    "pipeline": "nse_historical",
                    "layer": "bronze",
                    "table": self.table_name
                }
            )

            df = normalize_column_names(df)
            df = trim_and_nullify_strings(df)

            try:
                df, _ = enforce_schema(
                    df,
                    RAW_INPUT_SCHEMA,
                    logger=self.logger,
                    strict=self.raw_schema_strict,
                    stage="raw_input",
                )
            except Exception as e:
                raise SchemaEnforcementError("Raw input schema enforcement failed") from e

            return df

        except PipelineError:
            self.logger.exception("Extraction failed due to domain error")
            raise

        except PipelineException as e:
            self.logger.exception("Internal extraction failure")
            raise InputDataError("Failed to extract historical data") from e

       

    def transform(self, df_or_dfs, ctx) -> DataFrame:
        try:
            self.logger.info("Starting transformations")

            df = self._extract_df(df_or_dfs)

            dq_cfg = self.config.get("data_quality") or {}
            col_checks = build_column_checks(df, dq_cfg.get("column_checks", []))
            tbl_checks = build_table_checks(df, dq_cfg.get("table_checks", []))

            results = []
            checks = col_checks + tbl_checks

            if checks:
                try:
                    runner = DataQualityRunner(checks, self.logger)
                    results = runner.run()
                except Exception as e:
                    raise DQExecutionException("Column/Table DQ execution failed") from e

                self.metrics.gauge(
                    "pipeline.dq.checks.total",
                    len(results),
                    tags={"pipeline": "nse_daily_bhavcopy", "stage": "file"}
                )

                failed = [r for r in results if r.status.value == "FAIL"]

                self.metrics.gauge(
                    "pipeline.dq.checks.failed",
                    len(failed),
                    tags={"pipeline": "nse_daily_bhavcopy", "stage": "file"}
                )

                if failed:
                    self.metrics.increment(
                        "pipeline.dq.failure",
                        tags={"pipeline": pipeline_name, "layer": layer}
                    )
                    raise DataQualityError(
                        "Transform-stage data quality checks failed",
                        failed_checks=failed
                    )

            try:
                updt_df = apply_actions(self.spark, ctx, df, results)
            except Exception as e:
                raise TransformationException("Failed to apply DQ actions") from e

            res_df = updt_df or df
            res_df = self._derive_business_fields(res_df)
            res_df = self._add_metadata(res_df, ctx)

            try:
                res_df, _ = enforce_schema(
                    res_df,
                    EXPECTED_OUTPUT_SCHEMA,
                    logger=self.logger,
                    strict=self.output_schema_strict,
                    stage="output",
                )
            except Exception as e:
                raise SchemaEnforcementError("Output schema enforcement failed") from e

            row_count = safe_count(res_df, self.logger)
            self.metrics.gauge(
                "pipeline.transform.rows",
                row_count,
                tags={
                    "pipeline": "nse_historical",
                    "layer": "bronze",
                    "table": self.table_name
                }
            )

            return res_df

        except PipelineError:
            self.logger.exception("Transform failed due to domain error")
            raise

        except PipelineException as e:
            self.logger.exception("Internal transform failure")
            raise PipelineError("Transform step failed") from e
        
    def load(self, df):
        record_count = safe_count(df, logger=self.logger)

        self.metrics.gauge(
            "pipeline.load.rows",
            record_count,
            tags={
                "pipeline": "nse_historical",
                "layer": "bronze",
                "table": self.table_name
            }
        )
        if not record_count:
            self.logger.warning("No records to write. Skipping load.")
            self.metrics.increment(
                "pipeline.load.skipped",
                tags={
                    "pipeline": "nse_historical",
                    "layer": "bronze",
                    "table": self.table_name
                }
            )
            return

        try:
            df.writeTo(self.table_name).overwrite(sf.expr("true"))
            self.logger.info(f"Wrote {record_count:,} records")
            self.metrics.increment(
                "pipeline.load.success",
                tags={
                    "pipeline": "nse_historical",
                    "layer": "bronze",
                    "table": self.table_name
                }
            )
        except Exception as e:
            self.metrics.increment(
                "pipeline.load.failure",
                tags={
                    "pipeline": "nse_historical",
                    "layer": "bronze",
                    "table": self.table_name
                }
            )
            raise LoadError(
                f"Failed to write data to Iceberg table {self.table_name}"
            ) from e
    
    def create_execution_context(self):
        return super().create_execution_context()

    def run(self):
        ctx = self.create_execution_context()
        start_ts = time()

        self.logger.info(f"Execution Context: {process_context_to_string(ctx.process)}")
        load_type = self.config.get("load_type", "full")
        try:
            self.metrics.increment(
                "pipeline.run.started",
                tags={"pipeline": "nse_historical", "layer": "bronze"}
            )
            ctx.job_control.start_run(load_type=load_type)

            self.pre_etl(ctx)
            df = self.extract()
            df = self.transform(df, ctx)
            self.load(df)
            self.post_etl(ctx)

            rows_written = df.count()

            self.metrics.increment(
                "pipeline.run.success",
                tags={"pipeline": "nse_historical", "layer": "bronze"}
            )

            self.metrics.gauge(
                "pipeline.table.rows",
                rows_written,
                tags={
                    "pipeline": "nse_historical",
                    "layer": "bronze",
                    "table": self.table_name
                }
            )
            max_ts = df.agg({"trade_date": "max"}).collect()[0][0]
            ctx.job_control.mark_success(max_ts, rows_written)

        except PipelineError:
            self.metrics.increment(
                "pipeline.run.failure",
                tags={"pipeline": "nse_historical", "layer": "bronze", "type": "domain"}
            )
            raise

        except Exception:
            self.metrics.increment(
                "pipeline.run.failure",
                tags={"pipeline": "nse_historical", "layer": "bronze", "type": "unexpected"}
            )
            raise

        finally:
            self.metrics.timing(
                "pipeline.run.duration_ms",
                (time() - start_ts) * 1000,
                tags={"pipeline": "nse_historical", "layer": "bronze"}
            )

    def _extract_df(self, df_or_dfs) -> DataFrame:
        if isinstance(df_or_dfs, DataFrame):
            return df_or_dfs

        if isinstance(df_or_dfs, dict):
            if not df_or_dfs:
                raise InputDataError("df_or_dfs dict is empty")
            return next(iter(df_or_dfs.values()))

        raise InputDataError(
            "df_or_dfs must be a DataFrame or dict[str, DataFrame]"
        )



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

        df = df.withColumn("dividends", sf.col("dividends").cast("decimal(20,8)"))
        df = df.withColumn("stock_splits", sf.col("stock_splits").cast("decimal(20,8)"))

        # Add static columns
        for col, val in self.config.get("static_columns", {}).items():
            df = df.withColumn(col, sf.lit(val))

        return df

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
            "trade_year": sf.year("trade_date")
        }
        for col, expr in meta_cols.items():
            df = df.withColumn(col, expr)

        return df
    
    
    
    def _post_etl_cleanup(self, ctx):
        try:
            self.logger.info("Running Iceberg cleanup")

            one_hour_ago = ctx.process.process_timestamp - timedelta(days=1)
            timestamp_str = one_hour_ago.strftime("%Y-%m-%d %H:%M:%S")

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
            raise PostETLError("Post-ETL Iceberg cleanup failed")

@dataclass(frozen=True)
class PipelineArgs:
    env: str
    load_type: str
    is_backfill: bool
    run_id: Optional[str]
    process_id: Optional[str]
    input_pattern: Optional[str]
    
def build_pipeline_args(ns: argparse.Namespace) -> PipelineArgs:
    return PipelineArgs(
        env=ns.env,
        load_type=ns.load_type,
        is_backfill=ns.is_backfill,
        run_id=ns.run_id,
        process_id=ns.process_id,
        input_pattern=ns.input_pattern,
    )

def parse_args():
    parser = argparse.ArgumentParser(description="NSE Historical Data Pipeline")


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
    
# ==========================
# Main entrypoint
# ==========================
def main():
    # ==========================
    # Environment setup
    # ==========================
    args = parse_args()

    ENV = args.env
    conf_file = f"./config/ingestion/nse/historical/{ENV}_nse_historical_load.yaml"
    CONFIG = load_config(ENV, conf_file)
    
    # Apply command line overrides
    for attr in ["run_id", "process_id", "input_pattern", "execution_date", "is_backfill"]:
        val = getattr(args, attr, None)
        if val is not None:
            CONFIG[attr] = val
    


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

        # ==========================
        # Create pipeline and run
        # ==========================
        pipeline = PipelineFactory.get_pipeline("nse_historical", spark, logger, CONFIG)
        pipeline.run()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()