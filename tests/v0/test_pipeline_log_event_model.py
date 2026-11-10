from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from core.models.pipeline_log_event import PipelineLogEvent


def valid_started_event() -> dict[str, object]:
    return {
        "timestamp": datetime(2026, 1, 20, 9, tzinfo=UTC),
        "level": "INFO",
        "event": "pipeline_started",
        "run_id": "stage-bhavcopy-001",
        "pipeline_name": "stage_bhavcopy",
        "status": "RUNNING",
        "files_discovered": 0,
        "files_processed": 0,
        "valid_rows": 0,
        "rejected_rows": 0,
    }


def test_accepts_pipeline_started_event() -> None:
    event = PipelineLogEvent.model_validate(valid_started_event())

    assert event.event == "pipeline_started"
    assert event.status == "RUNNING"


def test_accepts_pipeline_completed_with_warnings_event() -> None:
    record = valid_started_event()
    record["level"] = "WARNING"
    record["event"] = "pipeline_completed_with_warnings"
    record["status"] = "COMPLETED_WITH_WARNINGS"
    record["files_discovered"] = 2
    record["files_processed"] = 1
    record["valid_rows"] = 1_818

    event = PipelineLogEvent.model_validate(record)

    assert event.level == "WARNING"
    assert event.status == "COMPLETED_WITH_WARNINGS"


def test_rejects_event_with_wrong_status() -> None:
    record = valid_started_event()
    record["status"] = "COMPLETED"

    with pytest.raises(
        ValidationError,
        match="pipeline_started events require status RUNNING",
    ):
        PipelineLogEvent.model_validate(record)


def test_rejects_failed_event_without_error_message() -> None:
    record = valid_started_event()
    record["level"] = "ERROR"
    record["event"] = "pipeline_failed"
    record["status"] = "FAILED"

    with pytest.raises(
        ValidationError,
        match="pipeline_failed events require error_message",
    ):
        PipelineLogEvent.model_validate(record)


def test_rejects_naive_event_timestamp() -> None:
    record = valid_started_event()
    record["timestamp"] = datetime(2026, 1, 20, 9)  # noqa: DTZ001

    with pytest.raises(
        ValidationError,
        match="timestamp must include timezone information",
    ):
        PipelineLogEvent.model_validate(record)
