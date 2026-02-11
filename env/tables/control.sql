CREATE TABLE IF NOT EXISTS local.control_db.job_run_audit (
  job_name STRING,
  run_id STRING,
  load_type STRING,
  status STRING,               -- STARTED / SUCCESS / FAILED
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  row_count BIGINT,
  error_message STRING
)
USING ICEBERG
PARTITIONED BY (job_name);


CREATE TABLE IF NOT EXISTS local.control_db.job_state (
  job_name STRING,
  last_successful_load_ts TIMESTAMP,
  last_run_id STRING,
  updated_at TIMESTAMP
)
USING ICEBERG;


CREATE TABLE IF NOT EXISTS local.control_db.metrics_events (
    ts            TIMESTAMP COMMENT 'Event timestamp (UTC)',
    metric_name   STRING    COMMENT 'Metric identifier, e.g. pipeline.table.rows',
    metric_type   STRING    COMMENT 'counter | gauge | timing',
    metric_value  DOUBLE    COMMENT 'Metric value',
    pipeline      STRING    COMMENT 'Pipeline name',
    table_name    STRING    COMMENT 'Logical table name (if applicable)',
    layer         STRING    COMMENT 'bronze | silver | gold',
    tags          MAP<STRING, STRING> COMMENT 'Free-form metric tags'
)
USING ICEBERG
PARTITIONED BY (
    days(ts),
    pipeline
);


