from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from core.models.daily_refresh import DailyRefreshResult


def valid_refresh_result() -> dict[str, object]:
    return {
        "run_id": "daily-refresh-001",
        "status": "COMPLETED",
        "started_at": datetime(2026, 1, 20, 9, tzinfo=UTC),
        "finished_at": datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        "files_discovered": 2,
        "files_processed": 2,
        "files_failed": 0,
        "duplicate_files_skipped": 0,
        "valid_rows": 3_000,
        "rejected_rows": 0,
        "freshness_status": "FRESH",
        "quality_status": "HEALTHY",
        "error_message": None,
    }


def test_accepts_completed_daily_refresh() -> None:
    result = DailyRefreshResult.model_validate(valid_refresh_result())

    assert result.status == "COMPLETED"
    assert result.freshness_status == "FRESH"
    assert result.quality_status == "HEALTHY"


def test_rejects_finished_before_started() -> None:
    record = valid_refresh_result()
    record["finished_at"] = datetime(2026, 1, 20, 8, 59, tzinfo=UTC)

    with pytest.raises(ValidationError, match="finished_at cannot be before"):
        DailyRefreshResult.model_validate(record)


def test_rejects_more_processed_and_failed_than_discovered() -> None:
    record = valid_refresh_result()
    record["files_processed"] = 2
    record["files_failed"] = 1

    with pytest.raises(ValidationError, match="cannot exceed discovered"):
        DailyRefreshResult.model_validate(record)


def test_rejects_failed_refresh_without_error_message() -> None:
    record = valid_refresh_result()
    record["status"] = "FAILED"

    with pytest.raises(ValidationError, match="require an error_message"):
        DailyRefreshResult.model_validate(record)


def test_rejects_error_message_for_completed_refresh() -> None:
    record = valid_refresh_result()
    record["error_message"] = "Unexpected failure."

    with pytest.raises(ValidationError, match="Only failed refreshes"):
        DailyRefreshResult.model_validate(record)
