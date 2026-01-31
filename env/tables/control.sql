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

