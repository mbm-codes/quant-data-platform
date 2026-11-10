from datetime import UTC, datetime
from pathlib import Path

from core.data.pipeline_run_store import write_pipeline_run
from core.models.pipeline_run import PipelineRun
from core.services.pipeline_run_status import get_pipeline_run_status


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


def test_loads_pipeline_run_status(tmp_path: Path) -> None:
    pipeline_runs_directory = tmp_path / "pipeline_runs"
    write_pipeline_run(make_completed_run(), pipeline_runs_directory)

    pipeline_run = get_pipeline_run_status(
        run_id="stage-bhavcopy-001",
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=pipeline_runs_directory,
    )

    assert pipeline_run is not None
    assert pipeline_run.status == "COMPLETED"
    assert pipeline_run.valid_rows == 1_818


def test_returns_none_when_no_pipeline_run_records_exist(
    tmp_path: Path,
) -> None:
    pipeline_run = get_pipeline_run_status(
        run_id="missing-run",
        database_path=tmp_path / "qdp.duckdb",
        pipeline_runs_directory=tmp_path / "pipeline_runs",
    )

    assert pipeline_run is None