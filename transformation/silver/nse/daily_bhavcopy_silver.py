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
from core.quality.factory.column_checks import build_column_checks
from core.quality.factory.table_checks import build_table_checks
from core.quality.runner import DataQualityRunner
from core.quality.actions import apply_actions
from core.exceptions.exceptions import (
    DQExecutionException,
    PipelineException,
    TransformationException,
)

from core.exceptions.errors import (
    InputDataError,
    DataQualityError,
    PipelineError,
    LoadError,
)

pipeline_name = "nse_daily_bhavcopy_silver"
layer = "silver"


class NSEDailyBhavcopySilver(SilverPipelineBase):
    def __init__(self, spark, logger, config, metrics):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.metrics = metrics
        self.input_table_names = config["input_tables"]
        self.output_table_name = config["output_table"]

    def pre_etl(self, ctx):
        self.logger.info("Running pre-etl steps for NSE Daily Bhavcopy Data Pipeline")
  
    
    def post_etl(self, ctx):
        self.logger.info("Running post-etl steps for NSE Daily Bhavcopy Data Pipeline")
        

    def extract(self) -> Union[DataFrame, dict]:
        try:
            self.logger.info(
                f"Reading {''.join(self.config['input_tables'].values())} bronze layer tables"
            )
            df_or_dfs = self.read_bronze()
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
            self.logger.exception("Failed to read bronze layer tables")
            raise InputDataError(
                "Failed to read bronze layer input tables"
            ) from e


    def transform(self, df_or_dfs, ctx) -> DataFrame:
        try:
            self.logger.info(
                f"Starting transformations on {''.join(self.config['input_tables'].values())}"
            )

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
                    raise DQExecutionException(
                        "Daily Bhavcopy silver DQ execution failed"
                    ) from e

                self.metrics.gauge(
                    "pipeline.dq.checks.total",
                    len(results),
                    tags={"pipeline": pipeline_name, "layer": layer}
                )
                failed = [r for r in results if r.status.value == "FAIL"]

                self.metrics.gauge(
                    "pipeline.dq.checks.failed",
                    len(failed),
                    tags={"pipeline": pipeline_name, "layer": layer}
                )


                if failed:
                    self.metrics.gauge(
                        "pipeline.dq.failure",
                        tags={"pipeline": pipeline_name, "layer": layer}
                    )

                    raise DataQualityError(
                        "Daily Bhavcopy silver data quality checks failed",
                        failed_checks=failed
                    )

            try:
                updt_df = apply_actions(self.spark, ctx, df, results)
            except Exception as e:
                raise TransformationException(
                    "Failed to apply DQ actions in daily bhavcopy silver transform"
                ) from e

            res_df = updt_df or df

            res_df = res_df.drop(
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

            res_df = self.apply_business_rules(res_df)
            res_df = self._add_metadata(res_df, ctx)

            row_count = safe_count(res_df, logger=self.logger)
            self.metrics.gauge(
                "pipeline.transform.rows",
                row_count,
                tags={
                        "pipeline": pipeline_name, 
                        "layer": layer,
                        "table": self.output_table_name
                    }
            )

            self.logger.debug(f"Transformed schema: {res_df.schema.simpleString()}")
            return res_df

        except PipelineError:
            self.logger.exception("Daily Bhavcopy silver transform failed (domain error)")
            raise

        except PipelineException as e:
            self.logger.exception("Internal daily bhavcopy silver transform failure")
            raise PipelineError(
                "Daily Bhavcopy silver transform failed"
            ) from e

    
    
    def load(self, df):
        record_count = safe_count(df, logger=self.logger)
        self.metrics.gauge(
            "pipeline.load.rows",
            record_count,
            tags={
                    "pipeline": pipeline_name, 
                    "layer": layer,
                    "table": self.output_table_name
                }
        )

        if not record_count:
            self.logger.warning("No records to write. Skipping load.")
            return

        try:
            df.writeTo(self.output_table_name).overwrite(sf.expr("true"))
            self.metrics.increment(
                "pipeline.load.success",
                tags={
                        "pipeline": pipeline_name, 
                        "layer": layer,
                        "table": self.output_table_name
                    }
            )
            self.logger.info(f"Wrote {record_count:,} records")
        except Exception as e:
            self.metrics.increment(
                "pipeline.load.failure",
                tags={
                        "pipeline": pipeline_name, 
                        "layer": layer,
                        "table": self.output_table_name
                    }
            )

            self.logger.exception(
                f"Failed writing daily bhavcopy silver data to {self.output_table_name}"
            )
            raise LoadError(
                f"Failed to write daily bhavcopy silver data to {self.output_table_name}"
            ) from e
    
    def read_bronze(self) -> Union[DataFrame, dict]:
        df_map = {}
        try:
            for tbl_typ, tbl_name in self.config["input_tables"].items():
                df_map[tbl_typ] = self.spark.read.table(tbl_name)
        except Exception as e:
            raise PipelineException(
                "Failed while reading bronze tables for daily bhavcopy silver"
            ) from e

        if len(df_map) == 1:
            return next(iter(df_map.values()))
        return df_map

    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        try:
            self.logger.info(
                f"Record count before filtering: {safe_count(df)}"
            )

            df_clean = df.filter(sf.col("trade_date") >= "2023-10-30")

            self.logger.info(
                f"Record count after filtering: {safe_count(df_clean)}"
            )

            return df_clean

        except Exception as e:
            raise TransformationException(
                "Failed while applying daily bhavcopy silver business rules"
            ) from e

    
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
        for col, expr in meta_cols.items():
            df = df.withColumn(col, expr)

        return df

    
    def write_silver(self, df: DataFrame):
        return super().write_silver(df)
    
    def run_quality_checks(self, df: DataFrame, ctx):
        return super().run_quality_checks(df, ctx)
    
    def read_silver(self) -> Union[DataFrame, dict]:
        return super().read_silver()
    
    def create_execution_context(self):
        return super().create_execution_context()

    def run(self):

        ctx = self.create_execution_context()
        start_ts = time.time()
        self.metrics.increment(
            "pipeline.run.started",
            tags={
                "pipeline": pipeline_name,
                "layer": layer
            }
        )

        self.logger.info(f"Execution Context: {process_context_to_string(ctx.process)}")

        load_type = self.config.get("load_type", "full")

        try:
            ctx.job_control.start_run(load_type=load_type)
            self.pre_etl(ctx)
            df = self.extract()
            df = self.transform(df, ctx)
            self.load(df)
            rows_written = safe_count(df)

            self.metrics.gauge(
                "pipeline.table.rows",
                rows_written,
                tags={
                    "pipeline": pipeline_name,
                    "layer": layer,
                    "table": self.output_table_name
                }
            )

            self.metrics.increment(
                "pipeline.run.success",
                tags={
                    "pipeline": pipeline_name,
                    "layer": layer
                }
            )
            self.post_etl(ctx)

            max_ts = df.agg({"trade_date": "max"}).collect()[0][0]
            

            ctx.job_control.mark_success(max_ts, rows_written)
            self.logger.info(
                f"Daily Bhavcopy silver pipeline completed. Rows written: {rows_written}"
            )

        except PipelineError as e:
            self.metrics.increment(
                "pipeline.run.failure",
                tags={
                    "pipeline": pipeline_name, "layer": layer
                }
            )
            ctx.job_control.mark_failure(str(e))
            self.logger.exception(
                "Daily Bhavcopy silver pipeline failed due to domain error"
            )
            raise

        except Exception:
            self.metrics.increment(
                "pipeline.run.failure",
                tags={
                    "pipeline": pipeline_name, "layer": layer
                }
            )
            ctx.job_control.mark_failure("Unexpected daily bhavcopy silver pipeline failure")
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
    
