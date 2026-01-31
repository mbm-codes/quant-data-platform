# tests/integration/test_nse_historical_pipeline.py
import os
from transformation.silver.nse.daily_bhavcopy_silver import NSEDailyBhavcopySilver
from core.logging.logging import QDPLogger
import yaml
from datetime import date
from core.context.process_context import create_process_context
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, functions as sf

def load_test_config():
    with open("tests/configs/test_nse_bhavcopy_daily_bronze_to_silver.yaml") as f:
        return yaml.safe_load(f)

def test_nse_bhavcopy_daily_end_to_end(spark):

    config = load_test_config()

    create_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {config["output_table"]} (
        symbol           STRING,
        series           STRING,
        trade_date            DATE,
        prev_close       DOUBLE,
        open_price       DOUBLE,
        high_price       DOUBLE,
        low_price        DOUBLE,
        last_price       DOUBLE,
        close_price      DOUBLE,
        avg_price        DOUBLE,
        ttl_trd_qnty     BIGINT,
        turnover_lacs    DOUBLE,
        no_of_trades     BIGINT,
        deliv_qty        BIGINT,
        deliv_per        DOUBLE,
        exchange STRING,            
        ingestion_ts TIMESTAMP,
        execution_date DATE,
        run_id STRING,
        job_name STRING,
        process_id STRING,
        pipeline_version STRING,
        trade_year INT
        ) USING PARQUET
        PARTITIONED BY (trade_year);
    """

    logger = QDPLogger(name="test_logger")

    pipeline = NSEDailyBhavcopySilver(
        spark=spark,
        logger=logger,
        config=config
    )

    ctx = pipeline.create_execution_context()
    proc_ctx = create_process_context(
        pipeline_version="v1.0",
        is_backfill=False,
        process_id="int_test_nse_bhavcopy_daily_brz_to_silver",
        run_id="1",
        process_name="int_test_nse_bhavcopy_daily_brz_to_silver",
        spark=spark,
        force_new_run_id=False,
        orchestrator_context=None
    )

    ctx.process = proc_ctx

    mock_data = [
        # Before 2023-10-30
        ("RELIANCE", "EQ", date(2023, 10, 25), 2400.0, 2410.0, 2430.0, 2390.0, 2420.0, 2415.0, 2412.0, 1000000, 24150.0, 120000, 600000, 60.0, "NSE"),
        ("RELIANCE", "EQ", date(2023, 10, 25), 2400.0, 2410.0, 2430.0, 2390.0, 2420.0, 2415.0, 2412.0, 1000000, 24150.0, 120000, 600000, 60.0, "NSE"),  # duplicate

        # On 2023-10-30
        ("TCS", "EQ", date(2023, 10, 30), 3500.0, 3520.0, 3550.0, 3490.0, 3540.0, 3535.0, 3530.0, 500000, 17675.0, 80000, 300000, 60.0, "NSE"),

        # After 2023-10-30
        ("INFY", "EQ", date(2023, 11, 2), 1500.0, 1510.0, 1525.0, 1495.0, 1515.0, 1512.0, 1510.0, 700000, 10584.0, 90000, 400000, 57.0, "NSE"),
        ("INFY", "EQ", date(2023, 11, 2), 1500.0, 1510.0, 1525.0, 1495.0, 1515.0, 1512.0, 1510.0, 700000, 10584.0, 90000, 400000, 57.0, "NSE"),  # duplicate
    ]

    cols_to_drop = [ "data_src","source_file","ingestion_ts",
                    "execution_date","run_id","job_name","process_id","pipeline_version",
                    "is_backfill","trade_year" ]

    spark.sql(f"drop table if exists {config['output_table']}")
    spark.sql(create_table_ddl)

    input_df = pipeline.extract()
    if isinstance(input_df, DataFrame):
        df = input_df
    elif isinstance(input_df, dict):
        df = next(iter(input_df.values()))
    else:
        raise ValueError("df_or_dfs must be a DataFrame or dict")
    
    schema = df.schema

    new_schema = StructType([f for f in schema.fields if f.name not in cols_to_drop])
    
    mock_df = spark.createDataFrame(mock_data, new_schema)
    df = df.filter(sf.col('trade_date') > '2024-01-10').limit(1).drop(*cols_to_drop)


    test_df = df.unionByName(mock_df)
    
    result_df = pipeline.transform(df_or_dfs=test_df, ctx=ctx)
    pipeline.load(result_df)
    
    df = spark.table(config["output_table"])

    assert df.count() == 3


    assert "symbol" in df.columns
    assert "series" in df.columns
    assert "trade_date" in df.columns
    assert "prev_close" in df.columns
    assert "open_price" in df.columns
    assert "high_price" in df.columns
    assert "low_price" in df.columns
    assert "last_price" in df.columns
    assert "close_price" in df.columns
    assert "avg_price" in df.columns
    assert "ttl_trd_qnty" in df.columns
    assert "turnover_lacs" in df.columns
    assert "no_of_trades" in df.columns
    assert "deliv_qty" in df.columns
    assert "deliv_per" in df.columns

    result = df.filter( df.trade_date >= "2023-10-30").count()
    assert result == 3

    result = df.filter( df.trade_date <= "2023-10-39").count()
    assert result == 0
   

    



