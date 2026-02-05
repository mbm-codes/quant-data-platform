import argparse
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
from core.quality.factory.column_checks import build_column_checks
from core.quality.factory.table_checks import build_table_checks
from core.quality.runner import DataQualityRunner

class NSEHistoricalSilver(SilverPipelineBase):
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.input_table_names = config["input_tables"]
        self.output_table_name = config["output_table"]

    def pre_etl(self, ctx):
        self.logger.info("Running pre-etl steps for NSE Historical Data Pipeline")
  
    
    def post_etl(self, ctx):
        self.logger.info("Running post-etl steps for NSE Historical Data Pipeline")
        

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

            dq_cfg = self.config.get("data_quality", None)
            col_dq_checks = []
            tbl_dq_checks = []
            col_checks = []
            tbl_checks = []

            if dq_cfg:
                col_dq_checks = dq_cfg.get("column_checks", [])
                tbl_dq_checks = dq_cfg.get("table_checks", [])
            
            col_checks = build_column_checks(df, col_dq_checks)
            tbl_checks = build_table_checks(df, tbl_dq_checks)
            runner = DataQualityRunner(col_checks + tbl_checks, self.logger)
            results = runner.run()

            for r in results:
                if r.status.value == "FAIL":
                    raise RuntimeError(f"DQ failed: {r.check_name} - {r.message}")


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
        # 1 filter out data on or after 2023-10-30
        # 2 do a dedup on entire dataset
        df_filtered = df.filter(sf.col("trade_date") < "2023-10-30")
        self.logger.info(f"Record count after filtering data out on or after 2023-10-30, {safe_count(df)}")
        
        df_clean = df_filtered.dropDuplicates(["trade_date", "symbol"])
        self.logger.info(f"Record count after deduping, {safe_count(df)}")
       
        return df_clean

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
    
    def create_execution_context(self):
        return super().create_execution_context()
    
    def read_silver(self) -> Union[DataFrame, dict]:
        return super().read_silver()

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
    conf_file = f"./config/transformation/{ENV}_slvr_nse_historical.yaml"
    CONFIG = load_config(ENV, conf_file)


    # Compute log paths early
    app_log_file = CONFIG.get("app_log_file") or f"./logs/{ENV}_slvr_nse_historical_app.log"
    spark_log_file = CONFIG.get("spark_log_file") or f"./logs/{ENV}_slvr_nse_historical_spark.log"

        # ==========================
    # Initialize logger
    # ==========================
    logger = QDPLogger(
        name=f"{ENV}_slvr_nse_historical",
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
            app_name=f"qdp-{ENV}-slvr-nse-historical",
            environment=ENV,
            warehouse_path=warehouse_path,
            catalog_name=catalog_name
        ).get_spark()

        logger.attach_spark_logs(spark)

        # ==========================
        # Create pipeline and run
        # ==========================
        pipeline = TransformationPipelineFactory.get_pipeline("nse_historical_silver", spark, logger, copy.deepcopy(CONFIG))
        pipeline.run()

    except Exception as e:
        logger.error(f"Transformation execution failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")



if __name__ == "__main__":
    main()
    
