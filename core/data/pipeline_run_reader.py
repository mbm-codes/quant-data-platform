import duckdb

from core.models.pipeline_run import PipelineRun

PIPELINE_RUN_COLUMNS = (
    "run_id",
    "pipeline_name",
    "input_dataset_version",
    "status",
    "started_at",
    "finished_at",
    "files_discovered",
    "files_processed",
    "valid_rows",
    "rejected_rows",
    "error_message",
)


def load_pipeline_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
) -> PipelineRun | None:
    """Load the current state record for one pipeline run."""
    rows = connection.execute(
        """
        SELECT
            run_id,
            pipeline_name,
            input_dataset_version,
            status,
            started_at,
            finished_at,
            files_discovered,
            files_processed,
            valid_rows,
            rejected_rows,
            error_message
        FROM pipeline_runs
        WHERE run_id = ?
        LIMIT 2
        """,
        [run_id],
    ).fetchall()

    if not rows:
        return None

    if len(rows) > 1:
        raise ValueError(f"Duplicate pipeline-run records found: {run_id}")

    return PipelineRun.model_validate(
        dict(zip(PIPELINE_RUN_COLUMNS, rows[0], strict=True))
    )

def load_latest_pipeline_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    pipeline_name: str,
) -> PipelineRun | None:
    """Load the most recently started run for one pipeline."""
    row = connection.execute(
        """
        SELECT
            run_id,
            pipeline_name,
            input_dataset_version,
            status,
            started_at,
            finished_at,
            files_discovered,
            files_processed,
            valid_rows,
            rejected_rows,
            error_message
        FROM pipeline_runs
        WHERE pipeline_name = ?
        ORDER BY started_at DESC, run_id DESC
        LIMIT 1
        """,
        [pipeline_name],
    ).fetchone()

    if row is None:
        return None

    return PipelineRun.model_validate(
        dict(zip(PIPELINE_RUN_COLUMNS, row, strict=True))
    )