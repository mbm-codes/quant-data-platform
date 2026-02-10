# tests/integration/test_nse_historical_pipeline.py
import os
from decimal import Decimal
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
        prev_close       DECIMAL(20,8),
        open_price       DECIMAL(20,8),
        high_price       DECIMAL(20,8),
        low_price        DECIMAL(20,8),
        last_price       DECIMAL(20,8),
        close_price      DECIMAL(20,8),
        avg_price        DECIMAL(20,8),
        ttl_trd_qnty     BIGINT,
        turnover_lacs    DECIMAL(20,8),
        no_of_trades     BIGINT,
        deliv_qty        BIGINT,
        deliv_per        DECIMAL(20,8),
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


    # Before 2023-10-30

    mock_data = [
    (
        "RELIANCE", "EQ", date(2023, 10, 25),
        Decimal("2400.00000000"), Decimal("2410.00000000"),
        Decimal("2430.00000000"), Decimal("2390.00000000"),
        Decimal("2420.00000000"), Decimal("2415.00000000"),
        Decimal("2412.00000000"),
        1000000,
        Decimal("24150.00000000"),
        120000, 600000,
        Decimal("60.00000000"),
        "NSE"
    ),
    (
        "RELIANCE", "EQ", date(2023, 10, 25),  # duplicate
        Decimal("2400.00000000"), Decimal("2410.00000000"),
        Decimal("2430.00000000"), Decimal("2390.00000000"),
        Decimal("2420.00000000"), Decimal("2415.00000000"),
        Decimal("2412.00000000"),
        1000000,
        Decimal("24150.00000000"),
        120000, 600000,
        Decimal("60.00000000"),
        "NSE"
    ),
    (
        "TCS", "EQ", date(2023, 10, 30),
        Decimal("3500.00000000"), Decimal("3520.00000000"),
        Decimal("3550.00000000"), Decimal("3490.00000000"),
        Decimal("3540.00000000"), Decimal("3535.00000000"),
        Decimal("3530.00000000"),
        500000,
        Decimal("17675.00000000"),
        80000, 300000,
        Decimal("60.00000000"),
        "NSE"
    ),
    (
        "INFY", "EQ", date(2023, 11, 2),
        Decimal("1500.00000000"), Decimal("1510.00000000"),
        Decimal("1525.00000000"), Decimal("1495.00000000"),
        Decimal("1515.00000000"), Decimal("1512.00000000"),
        Decimal("1510.00000000"),
        700000,
        Decimal("10584.00000000"),
        90000, 400000,
        Decimal("57.00000000"),
        "NSE"
    ),
    (
        "INFY", "EQ", date(2023, 11, 2),  # duplicate
        Decimal("1500.00000000"), Decimal("1510.00000000"),
        Decimal("1525.00000000"), Decimal("1495.00000000"),
        Decimal("1515.00000000"), Decimal("1512.00000000"),
        Decimal("1510.00000000"),
        700000,
        Decimal("10584.00000000"),
        90000, 400000,
        Decimal("57.00000000"),
        "NSE"
        ),
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
    result_df = result_df.dropDuplicates(['trade_date', 'symbol', 'series'])
    
    result_df.show(truncate=False)
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

    result = df.filter( df.trade_date < "2023-10-30").count()
    assert result == 0
   

    



