from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from core.models.pipeline_run import PipelineRun


def valid_running_run() -> dict[str, object]:
    return {
        "run_id": "stage-bhavcopy-001",
        "pipeline_name": "stage_bhavcopy",
        "input_dataset_version": "nse-bhavcopy-v1",
        "status": "RUNNING",
        "started_at": datetime(2026, 1, 20, 9, tzinfo=UTC),
        "files_discovered": 1,
        "files_processed": 0,
        "valid_rows": 0,
        "rejected_rows": 0,
    }


def test_accepts_running_pipeline_run() -> None:
    run = PipelineRun.model_validate(valid_running_run())

    assert run.status == "RUNNING"
    assert run.finished_at is None
    assert run.error_message is None


def test_accepts_completed_pipeline_run() -> None:
    record = valid_running_run()
    record["status"] = "COMPLETED"
    record["finished_at"] = datetime(2026, 1, 20, 9, 2, tzinfo=UTC)
    record["files_processed"] = 1
    record["valid_rows"] = 1_818

    run = PipelineRun.model_validate(record)

    assert run.status == "COMPLETED"
    assert run.valid_rows == 1_818


def test_accepts_failed_pipeline_run() -> None:
    record = valid_running_run()
    record["status"] = "FAILED"
    record["finished_at"] = datetime(2026, 1, 20, 9, 1, tzinfo=UTC)
    record["error_message"] = "Unable to read source file."

    run = PipelineRun.model_validate(record)

    assert run.status == "FAILED"
    assert run.error_message == "Unable to read source file."


def test_rejects_completed_run_without_finished_time() -> None:
    record = valid_running_run()
    record["status"] = "COMPLETED"

    with pytest.raises(
        ValidationError,
        match="COMPLETED pipeline runs require finished_at",
    ):
        PipelineRun.model_validate(record)


def test_rejects_failed_run_without_error_message() -> None:
    record = valid_running_run()
    record["status"] = "FAILED"
    record["finished_at"] = datetime(2026, 1, 20, 9, 1, tzinfo=UTC)

    with pytest.raises(
        ValidationError,
        match="FAILED pipeline runs require error_message",
    ):
        PipelineRun.model_validate(record)


def test_rejects_finished_time_before_start_time() -> None:
    record = valid_running_run()
    record["status"] = "COMPLETED"
    record["finished_at"] = (
        datetime(2026, 1, 20, 9, tzinfo=UTC) - timedelta(seconds=1)
    )

    with pytest.raises(
        ValidationError,
        match="finished_at cannot be before started_at",
    ):
        PipelineRun.model_validate(record)

def test_accepts_completed_pipeline_run_with_warnings() -> None:
    record = valid_running_run()
    record["status"] = "COMPLETED_WITH_WARNINGS"
    record["finished_at"] = datetime(2026, 1, 20, 9, 2, tzinfo=UTC)
    record["files_discovered"] = 2
    record["files_processed"] = 1
    record["valid_rows"] = 1_818

    run = PipelineRun.model_validate(record)

    assert run.status == "COMPLETED_WITH_WARNINGS"