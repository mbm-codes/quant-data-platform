import argparse
import time
from core.pipeline.transformation_base import SilverPipelineBase
from core.pipeline.transformation_factory import TransformationPipelineFactory
from pyspark.sql import DataFrame, functions as sf
from typing import Union
from core.spark_dataframe.actions import safe_count
from core.context.process_context import create_process_context, process_context_to_string
from core.config.loader import load_config
from core.logging.logging import QDPLogger
from lakehouse.iceberg.spark_session import SparkSessionBuilder
from datetime import datetime, timezone
import os
import copy
from pyspark.sql.window import Window
from functools import reduce
from core.exceptions.exceptions import (
    PipelineException,
    TransformationException,
)

from core.exceptions.errors import (
    InputDataError,
    PipelineError,
    LoadError,
)


pipeline_name = "nse_equity_silver"
layer = "silver"

class NSEEquityDataSilver(SilverPipelineBase):
    def __init__(self, spark, logger, config, metrics):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.metrics = metrics
        self.input_table_names = config["input_tables"]
        self.output_table_name = config["output_table"]

    def pre_etl(self, ctx):
        self.logger.info("Running pre-etl steps for NSE Equity Silver Data Pipeline")
  
    
    def post_etl(self, ctx):
        self.logger.info("Running post-etl steps for NSE Equity Silver Data Pipeline")
        

    def extract(self) -> Union[DataFrame, dict]:
        try:
            self.logger.info(
                f"Reading {''.join(self.config['input_tables'].values())} silver layer tables"
            )
            df_or_dfs = self.read_silver()
            if isinstance(df_or_dfs, DataFrame):
                rows = safe_count(df_or_dfs, logger=self.logger)
                self.metrics.gauge(
                    "pipeline.extract.rows",
                    rows,
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer,
                        "table": "input"
                    }
                )
            else:
                for tbl, df in df_or_dfs.items():
                    rows = safe_count(df, logger=self.logger)
                    self.metrics.gauge(
                        "pipeline.extract.rows",
                        rows,
                        tags={
                            "pipeline": pipeline_name,
                            "layer": layer,
                            "table": tbl
                        }
                    )
            return df_or_dfs
        
        except Exception as e:
            self.logger.exception("Failed to read silver layer input tables")
            raise InputDataError(
                "Failed to read silver layer input tables"
            ) from e

    def transform(self, df_or_dfs, ctx) -> DataFrame:
        try:
            self.logger.info(
                f"Starting transformations on {''.join(self.config['input_tables'].values())}"
            )

            if not isinstance(df_or_dfs, dict) or not df_or_dfs:
                raise InputDataError(
                    "Equity silver transform expects a non-empty dict of DataFrames"
                )

            try:
                cleaned_inputs = {
                    k: v.drop(
                        "source_file",
                        "ingestion_ts",
                        "execution_date",
                        "run_id",
                        "job_name",
                        "process_id",
                        "pipeline_version",
                        "is_backfill",
                        "trade_year",
                    )
                    for k, v in df_or_dfs.items()
                }

                cleaned_inputs["slvr_nse_historical"] = (
                    cleaned_inputs["slvr_nse_historical"]
                    .withColumnRenamed("volume_qty", "ttl_trd_qnty")
                    .withColumn(
                        "symbol",
                        sf.regexp_replace("symbol", r"\.NS$", "")
                    )
                )

                merged_df = reduce(
                    lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True),
                    cleaned_inputs.values(),
                )

            except Exception as e:
                raise TransformationException(
                    "Failed during equity input normalization or merge"
                ) from e

            merged_df = self.apply_business_rules(merged_df)
            merged_df = self._add_metadata(merged_df, ctx)

            row_count = safe_count(merged_df)
            self.metrics.gauge(
                    "pipeline.transform.rows",
                    row_count,
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer
                    }
                )

            self.logger.debug(
                f"Transformed schema: {merged_df.schema.simpleString()}"
            )

            return merged_df

        except PipelineError:
            self.logger.exception("Equity silver transform failed due to domain error")
            raise

        except PipelineException as e:
            self.logger.exception("Internal equity silver transform failure")
            raise PipelineError(
                "Equity silver transform failed"
            ) from e
    
    
    def load(self, df):
        record_count = safe_count(df, logger=self.logger)

        if not record_count:
            self.logger.warning("No records to write. Skipping load.")
            return

        try:
            df.writeTo(self.output_table_name).overwrite(sf.expr("true"))

            self.metrics.gauge(
                    "pipeline.load.rows",
                    record_count,
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer,
                        "table": self.output_table_name
                    }
            )
            self.logger.info(f"Wrote {record_count:,} records")
        except Exception as e:
            self.logger.exception(
                f"Failed writing equity silver data to {self.output_table_name}"
            )
            raise LoadError(
                f"Failed to write equity silver data to {self.output_table_name}"
            ) from e

    
    def read_silver(self) -> Union[DataFrame, dict]:
        df_map = {}
        try:
            for tbl_typ, tbl_name in self.config["input_tables"].items():
                if tbl_typ == "slvr_nse_bhavcopy_daily":
                    df_map[tbl_typ] = (
                        self.spark.read.table(tbl_name)
                        .filter("series = 'EQ'")
                        .drop("series")
                    )
                else:
                    df_map[tbl_typ] = self.spark.read.table(tbl_name)
        except Exception as e:
            raise PipelineException(
                "Failed while reading silver tables for equity merge"
            ) from e

        return df_map

    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        try:
            self.logger.info(
                f"Record count before deduping: {safe_count(df)}"
            )

            df_clean = df.dropDuplicates(["symbol", "trade_date"])

            self.logger.info(
                f"Record count after deduping: {safe_count(df_clean)}"
            )
            return df_clean

        except Exception as e:
            raise TransformationException(
                "Failed while applying equity silver business rules"
            ) from e

    def _add_metadata(self, df, ctx):
        # Metadata enrichment
        meta_cols = {
            "ingestion_ts": sf.lit(ctx.process.process_timestamp),
            "execution_date": sf.lit(ctx.process.process_date),
            "run_id": sf.lit(ctx.process.run_id),
            "job_name": sf.lit(ctx.process.process_name),
            "process_id": sf.lit(ctx.process.process_id),
            "pipeline_version": sf.lit(ctx.process.pipeline_version),
            "trade_year": sf.year("trade_date")
        }

        df = reduce(lambda acc_df, col_expr: acc_df.withColumn(col_expr[0], col_expr[1]), meta_cols.items(), df)


        return df

    def read_bronze(self) -> Union[DataFrame, dict]:
        return super().read_bronze()

    
    def write_silver(self, df: DataFrame):
        return super().write_silver(df)
    
    def run_quality_checks(self, df: DataFrame, ctx):
        return super().run_quality_checks(df, ctx)

    def create_execution_context(self):
        return super().create_execution_context()
    
    def run(self):
        start_ts = time.time()
        ctx = self.create_execution_context()
        self.logger.info(f"Execution Context: {process_context_to_string(ctx.process)}")
        self.metrics.increment(
            "pipeline.run.started",
            tags={
                "pipeline": pipeline_name,
                "layer": layer
            }
        )

        load_type = self.config.get("load_type", "full")

        try:
            ctx.job_control.start_run(load_type=load_type)
            self.pre_etl(ctx)
            df = self.extract()
            df = self.transform(df, ctx)
            self.load(df)
            self.post_etl(ctx)

            max_ts = df.agg({"trade_date": "max"}).collect()[0][0]
            rows_written = df.count()

            ctx.job_control.mark_success(max_ts, rows_written)
            self.logger.info(
                f"Equity silver pipeline completed. Rows written: {rows_written}"
            )

            self.metrics.increment(
                "pipeline.run.success",
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer
                    }
                )

        except PipelineError as e:
            self.metrics.increment(
                "pipeline.run.failure",
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer
                    }
            )


            ctx.job_control.mark_failure(str(e))
            self.logger.exception(
                "Equity silver pipeline failed due to domain error"
            )
            raise

        except Exception:
            self.metrics.increment(
                "pipeline.run.failure",
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer
                    }
            )

            ctx.job_control.mark_failure("Unexpected equity silver pipeline failure")
            self.logger.exception("Unexpected failure")
            raise
        finally:
            self.metrics.gauge(
                "pipeline.run.duration_ms",
                (time.time() - start_ts) * 1000,
                    tags={
                        "pipeline": pipeline_name,
                        "layer": layer,
                        "table": self.output_table_name
                    }
                )



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


def main():
    # ==========================
    # Environment setup
    # ==========================

    args = parse_args()

    ENV = os.getenv("QDP_ENV", "local")
    conf_file = f"./config/transformation/{ENV}_slvr_nse_merge_historical_daily.yaml"
    CONFIG = load_config(ENV, conf_file)


    # Compute log paths early
    app_log_file = CONFIG.get("app_log_file") or f"./logs/{ENV}_slvr_nse_merge_historical_daily_app.log"
    spark_log_file = CONFIG.get("spark_log_file") or f"./logs/{ENV}_slvr_nse_merge_historical_daily_spark.log"

        # ==========================
    # Initialize logger
    # ==========================
    logger = QDPLogger(
        name=f"{ENV}_slvr_nse_merge_historical_daily",
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
            app_name=f"qdp-{ENV}-slvr-nse-merge-historical-daily",
            environment=ENV,
            warehouse_path=warehouse_path,
            catalog_name=catalog_name
        ).get_spark()

        logger.attach_spark_logs(spark)
        start_ts = datetime.now(timezone.utc)
        logger.info(f"Starting Silver NSE Equity Silver transformation in [{ENV}] at {start_ts}")

        # ==========================
        # Create pipeline and run
        # ==========================
        pipeline = TransformationPipelineFactory.get_pipeline("nse_equity_silver", spark, logger, copy.deepcopy(CONFIG))
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
    
