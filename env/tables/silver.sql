
CREATE TABLE IF NOT EXISTS local.market_lakehouse.slvr_nse_historical_prices_till_29_oct_2023 (
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
    ingestion_ts TIMESTAMP,
    execution_date DATE,
    run_id STRING,
    job_name STRING,
    process_id STRING,
    pipeline_version STRING,
    trade_year INT
) USING PARQUET
PARTITIONED BY (trade_year);



CREATE TABLE IF NOT EXISTS local.market_lakehouse.slvr_nse_bhavcopy_daily_from_30_oct_2023 (
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
    data_src STRING,                
    ingestion_ts TIMESTAMP,
    execution_date DATE,
    run_id STRING,
    job_name STRING,
    process_id STRING,
    pipeline_version STRING,
    trade_year INT
) USING PARQUET
PARTITIONED BY (trade_year);


CREATE TABLE IF NOT EXISTS local.market_lakehouse.slvr_nse_equity_data (
    symbol           STRING,
    trade_date       DATE,
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
    dividends        DECIMAL(20,8),       
    stock_splits     DECIMAL(20,8),       
    exchange STRING,
    data_src STRING,                
    ingestion_ts TIMESTAMP,
    execution_date DATE,
    run_id STRING,
    job_name STRING,
    process_id STRING,
    pipeline_version STRING,
    trade_year INT
) USING PARQUET
PARTITIONED BY (trade_year);