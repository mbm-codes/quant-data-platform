# tests/integration/test_nse_historical_pipeline.py
import os
from ingestion.nse.historical.historical_loads import NSEHistoricalDataPipeline
from core.logging.logging import QDPLogger
from core.metrics.spark import SparkMetricsEmitter
from core.context.process_context import create_process_context

def test_pipeline_end_to_end(spark, tmp_path):
    input_path = tmp_path / "input"
    input_path.mkdir()

    # Copy test CSV
    test_csv = os.path.join(
        os.path.dirname(__file__),
        "data/sample_nse.csv.gz"
    )
    os.system(f"cp {test_csv} {input_path}")

    config = {
        "input_pattern": str(input_path),
        "output_table": "local.market_lakehouse.intgr_bronze_nse_historical_prices_raw",
        "allow_destructive": True,
        "pipeline_version": "test",
        "process_id": "test",
        "run_id": "1",
        "process_name": "test",
        "casts": {
            "open": ("open_price", "DECIMAL(20,8)"),
            "high": ("high_price", "DECIMAL(20,8)"),
            "low": ("low_price", "DECIMAL(20,8)"),
            "close": ("close_price", "DECIMAL(20,8)"),
            "volume": ("volume_qty", "long"),
        },
        "static_columns": {
            "exchange": "NSE",
            "data_src": "yahoo"
        },
        "schema": {
            "raw": {"strict": True},
            "output": {"strict": True},
        },
    }

    create_table_ddl = """
        CREATE TABLE IF NOT EXISTS local.market_lakehouse.intgr_bronze_nse_historical_prices_raw (
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
    ENV = os.getenv("QDP_ENV", "local")
    pipeline_type = "nse_historical_test"

    metrics = SparkMetricsEmitter(
            spark,
            table_name="local.control_db.metrics_events",
            default_tags={
                "env": ENV,
                "pipeline": pipeline_type
            }
    )
    logger = QDPLogger(name="test_logger")

    pipeline = NSEHistoricalDataPipeline(
        spark=spark,
        logger=logger,
        config=config,
        metrics=metrics
    )

    ctx = pipeline.create_execution_context()
    proc_ctx = create_process_context(
        pipeline_version="v1.0",
        is_backfill=False,
        process_id="int_test_nse_historical",
        run_id="1",
        process_name="int_test_nse_historical_bronze",
        spark=spark,
        force_new_run_id=False,
        orchestrator_context=None
    )

    ctx.process = proc_ctx
    
    spark.sql(create_table_ddl)
    pipeline.run()

    df = spark.table(config["output_table"])

    assert df.count() == 2
    assert "trade_date" in df.columns
    assert "symbol" in df.columns
    assert "open_price" in df.columns
    assert "high_price" in df.columns
    assert "low_price" in df.columns
    assert "close_price" in df.columns
    assert "volume_qty" in df.columns
    assert "exchange" in df.columns
    assert "data_src" in df.columns
    assert "trade_year" in df.columns

    result = df.filter((df.symbol == "SAMPLE_NSE") & (df.trade_date=="2022-01-01")).collect()
    assert result[0]["open_price"] == 100
    assert result[0]["high_price"] == 110
    assert result[0]["low_price"] == 90
    assert result[0]["close_price"] == 105
    assert result[0]["volume_qty"] == 1000
    



