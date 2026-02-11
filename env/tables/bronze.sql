

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



CREATE TABLE IF NOT EXISTS local.market_lakehouse.bronze_nse_daily_bhavcopy_raw (
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


CREATE TABLE IF NOT EXISTS local.market_lakehouse.quarantine_records (
    run_id              STRING,
    dataset_name        STRING,
    check_name          STRING,
    dq_status           STRING,      -- PASS / WARN / FAIL
    dq_action           STRING,      -- OBSERVE / QUARANTINE / CLEAN / BLOCK
    reason              STRING,
    invalid_predicate   STRING,
    row_count           BIGINT,
    sample_rows         ARRAY<MAP<STRING, STRING>>,
    created_at          TIMESTAMP
)
USING ICEBERG
PARTITIONED BY (
    days(created_at),
    dataset_name
);
 