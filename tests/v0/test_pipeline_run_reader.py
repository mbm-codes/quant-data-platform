from datetime import UTC, datetime
from pathlib import Path

from core.data.duckdb import connect, register_pipeline_runs_view
from core.data.pipeline_run_reader import load_pipeline_run
from core.data.pipeline_run_store import write_pipeline_run
from core.models.pipeline_run import PipelineRun
from core.data.pipeline_run_reader import load_latest_pipeline_run

def make_completed_run() -> PipelineRun:
    return PipelineRun(
        run_id="stage-bhavcopy-001",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        status="COMPLETED",
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        files_discovered=1,
        files_processed=1,
        valid_rows=1_818,
        rejected_rows=0,
    )


def test_loads_pipeline_run_by_run_id(tmp_path: Path) -> None:
    pipeline_runs_directory = tmp_path / "pipeline_runs"
    write_pipeline_run(make_completed_run(), pipeline_runs_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_pipeline_runs_view(
            connection,
            pipeline_runs_directory,
        )
        pipeline_run = load_pipeline_run(
            connection,
            run_id="stage-bhavcopy-001",
        )
    finally:
        connection.close()

    assert pipeline_run is not None
    assert pipeline_run.status == "COMPLETED"
    assert pipeline_run.valid_rows == 1_818


def test_returns_none_for_unknown_pipeline_run(tmp_path: Path) -> None:
    pipeline_runs_directory = tmp_path / "pipeline_runs"
    write_pipeline_run(make_completed_run(), pipeline_runs_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_pipeline_runs_view(
            connection,
            pipeline_runs_directory,
        )
        pipeline_run = load_pipeline_run(
            connection,
            run_id="missing-run",
        )
    finally:
        connection.close()

    assert pipeline_run is None

def test_loads_latest_pipeline_run_by_pipeline_name(
    tmp_path: Path,
) -> None:
    pipeline_runs_directory = tmp_path / "pipeline_runs"

    older_run = make_completed_run()
    newer_run = older_run.model_copy(
        update={
            "run_id": "stage-bhavcopy-002",
            "started_at": datetime(2026, 1, 21, 9, tzinfo=UTC),
            "finished_at": datetime(2026, 1, 21, 9, 2, tzinfo=UTC),
        }
    )

    write_pipeline_run(older_run, pipeline_runs_directory)
    write_pipeline_run(newer_run, pipeline_runs_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_pipeline_runs_view(
            connection,
            pipeline_runs_directory,
        )
        pipeline_run = load_latest_pipeline_run(
            connection,
            pipeline_name="stage_bhavcopy",
        )
    finally:
        connection.close()

    assert pipeline_run is not None
    assert pipeline_run.run_id == "stage-bhavcopy-002"