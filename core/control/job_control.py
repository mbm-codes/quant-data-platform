from core.context.process_context import ProcessContext


class JobControl:
    def __init__(self, spark, process_context: ProcessContext ):
        self.spark = spark
        self.proc_ctx = process_context
    
    def start_run(self, load_type):
        self.run_id = self.proc_ctx.run_id
        ts_str = self.proc_ctx.process_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        self.spark.sql(f"""
            INSERT INTO local.control_db.job_run_audit
            VALUES (
                    '{self.proc_ctx.process_name}',
                    '{self.proc_ctx.run_id}',
                    '{load_type}',
                    'STARTED',
                    TIMESTAMP '{ts_str}',
                    NULL,
                    NULL,
                    NULL
                )
        """)

        return self.run_id
    
    def get_watermark(self):
        df = self.spark.sql(f"""
            SELECT last_successful_load_ts 
            FROM local.control_db.job_state
            WHERE job_name = '{self.proc_ctx.process_name}'
        """)
        return df.collect()[0][0] if df.count() > 0 else None
    
    def mark_success(self, max_ts, rows_written):
        self.spark.sql(f"""
            UPDATE local.control_db.job_run_audit
            SET status = 'SUCCESS',
                end_time = current_timestamp(),
                row_count = {rows_written}
            WHERE job_name = '{self.proc_ctx.process_name}'
                AND run_id = '{self.run_id}'
        """)

        self.spark.sql(f"""
            MERGE INTO local.control_db.job_state t
            USING (
                    SELECT 
                       '{self.proc_ctx.process_name}' AS job_name,
                       TIMESTAMP '{max_ts}' AS last_successful_load_ts,
                       '{self.run_id}' AS last_run_id,
                       current_timestamp() AS updated_at
              ) s
              ON t.job_name = s.job_name
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *
        """)

    def mark_failure(self, error_msg):
        self.spark.sql(f"""
            UPDATE local.control_db.job_run_audit
            SET status = 'FAILED',
                end_time = current_timestamp(),
                error_message = '{error_msg}'
            WHERE job_name = '{self.proc_ctx.process_name}'
                AND run_id = '{self.run_id}'
        """)