from pathlib import Path

from core.data.duckdb import connect, register_pipeline_runs_view
from core.data.pipeline_run_reader import load_pipeline_run
from core.models.pipeline_run import PipelineRun


def get_pipeline_run_status(
    *,
    run_id: str,
    database_path: Path,
    pipeline_runs_directory: Path,
) -> PipelineRun | None:
    """Return the current stored state for one pipeline run."""
    if not any(pipeline_runs_directory.glob("started_year=*/*.parquet")):
        return None

    connection = connect(database_path)

    try:
        register_pipeline_runs_view(
            connection,
            pipeline_runs_directory,
        )
        return load_pipeline_run(connection, run_id=run_id)
    finally:
        connection.close()