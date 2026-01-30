

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


CREATE TABLE IF NOT EXISTS local.market_lakehouse.intgr_bronze_nse_historical_prices_raw (
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



CREATE TABLE IF NOT EXISTS local.market_lakehouse.bronze_nse_daily_bhavcopy_raw (
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
)
USING PARQUET 
PARTITIONED BY (trade_year);

