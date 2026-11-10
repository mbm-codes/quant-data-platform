from datetime import datetime
from pathlib import Path

from core.data.pipeline_run_store import write_pipeline_run
from core.models.pipeline_run import PipelineRun


def start_pipeline_run(
    *,
    run_id: str,
    pipeline_name: str,
    input_dataset_version: str,
    output_directory: Path,
    started_at: datetime,
) -> PipelineRun:
    """Create and persist a RUNNING pipeline-run state."""
    pipeline_run = PipelineRun(
        run_id=run_id,
        pipeline_name=pipeline_name,
        input_dataset_version=input_dataset_version,
        status="RUNNING",
        started_at=started_at,
        files_discovered=0,
        files_processed=0,
        valid_rows=0,
        rejected_rows=0,
    )

    write_pipeline_run(pipeline_run, output_directory)

    return pipeline_run


def complete_pipeline_run(
    pipeline_run: PipelineRun,
    *,
    output_directory: Path,
    finished_at: datetime,
    files_discovered: int,
    files_processed: int,
    valid_rows: int,
    rejected_rows: int,
) -> PipelineRun:
    """Persist the completed state and final metrics for a pipeline run."""
    if pipeline_run.status != "RUNNING":
        raise ValueError("Only RUNNING pipeline runs can be completed.")

    status = (
        "COMPLETED_WITH_WARNINGS"
        if (
            files_processed < files_discovered
            or rejected_rows > 0
        )
        else "COMPLETED"
    )
    completed_run = pipeline_run.model_copy(
        update={
            "status": status,
            "finished_at": finished_at,
            "files_discovered": files_discovered,
            "files_processed": files_processed,
            "valid_rows": valid_rows,
            "rejected_rows": rejected_rows,
        }
    )

    write_pipeline_run(completed_run, output_directory)

    return completed_run


def fail_pipeline_run(
    pipeline_run: PipelineRun,
    *,
    output_directory: Path,
    finished_at: datetime,
    error_message: str,
    files_discovered: int,
    files_processed: int,
    valid_rows: int,
    rejected_rows: int,
) -> PipelineRun:
    """Persist a failed state, error message, and partial-run metrics."""
    if pipeline_run.status != "RUNNING":
        raise ValueError("Only RUNNING pipeline runs can be failed.")

    failed_run = pipeline_run.model_copy(
        update={
            "status": "FAILED",
            "finished_at": finished_at,
            "files_discovered": files_discovered,
            "files_processed": files_processed,
            "valid_rows": valid_rows,
            "rejected_rows": rejected_rows,
            "error_message": error_message,
        }
    )

    write_pipeline_run(failed_run, output_directory)

    return failed_run