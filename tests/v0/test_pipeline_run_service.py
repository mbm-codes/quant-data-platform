from datetime import UTC, datetime
from pathlib import Path

from core.services.pipeline_runs import (
    complete_pipeline_run,
    fail_pipeline_run,
    start_pipeline_run,
)


def test_starts_and_completes_pipeline_run(tmp_path: Path) -> None:
    started_at = datetime(2026, 1, 20, 9, tzinfo=UTC)

    running_run = start_pipeline_run(
        run_id="stage-bhavcopy-001",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        output_directory=tmp_path,
        started_at=started_at,
    )

    assert running_run.status == "RUNNING"
    assert len(list(tmp_path.rglob("*.parquet"))) == 1

    completed_run = complete_pipeline_run(
        running_run,
        output_directory=tmp_path,
        finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        files_discovered=1,
        files_processed=1,
        valid_rows=1_818,
        rejected_rows=0,
    )

    assert completed_run.status == "COMPLETED"
    assert completed_run.valid_rows == 1_818
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_marks_pipeline_run_as_failed(tmp_path: Path) -> None:
    running_run = start_pipeline_run(
        run_id="stage-bhavcopy-002",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        output_directory=tmp_path,
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
    )

    failed_run = fail_pipeline_run(
        running_run,
        output_directory=tmp_path,
        finished_at=datetime(2026, 1, 20, 9, 1, tzinfo=UTC),
        error_message="Unable to read source file.",
        files_discovered=1,
        files_processed=0,
        valid_rows=0,
        rejected_rows=0,
    )

    assert failed_run.status == "FAILED"
    assert failed_run.error_message == "Unable to read source file."

def test_marks_partial_pipeline_run_as_completed_with_warnings(
    tmp_path: Path,
) -> None:
    running_run = start_pipeline_run(
        run_id="stage-bhavcopy-003",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        output_directory=tmp_path,
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
    )

    completed_run = complete_pipeline_run(
        running_run,
        output_directory=tmp_path,
        finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        files_discovered=2,
        files_processed=1,
        valid_rows=1_818,
        rejected_rows=0,
    )

    assert completed_run.status == "COMPLETED_WITH_WARNINGS"