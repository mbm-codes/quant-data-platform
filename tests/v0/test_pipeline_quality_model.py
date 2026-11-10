from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.pipeline_quality import PipelineQualityResult


def valid_quality_result() -> dict[str, object]:
    return {
        "pipeline_name": "stage_bhavcopy",
        "latest_run_id": "stage-bhavcopy-001",
        "latest_run_status": "COMPLETED",
        "files_discovered": 1,
        "files_processed": 1,
        "valid_rows": 1_818,
        "rejected_rows": 0,
        "rejection_rate": Decimal("0"),
        "warning_rejection_rate": Decimal("0.01"),
        "critical_rejection_rate": Decimal("0.05"),
        "status": "HEALTHY",
        "checked_at": datetime(2026, 1, 20, 18, tzinfo=UTC),
    }


def test_accepts_healthy_pipeline_quality_result() -> None:
    result = PipelineQualityResult.model_validate(valid_quality_result())

    assert result.status == "HEALTHY"


def test_accepts_warning_result_for_completed_with_warnings() -> None:
    record = valid_quality_result()
    record["latest_run_status"] = "COMPLETED_WITH_WARNINGS"
    record["status"] = "WARNING"

    result = PipelineQualityResult.model_validate(record)

    assert result.status == "WARNING"


def test_accepts_critical_result_for_failed_pipeline() -> None:
    record = valid_quality_result()
    record["latest_run_status"] = "FAILED"
    record["status"] = "CRITICAL"

    result = PipelineQualityResult.model_validate(record)

    assert result.status == "CRITICAL"


def test_accepts_missing_quality_result() -> None:
    record = valid_quality_result()
    record["latest_run_id"] = None
    record["latest_run_status"] = None
    record["files_discovered"] = None
    record["files_processed"] = None
    record["valid_rows"] = None
    record["rejected_rows"] = None
    record["rejection_rate"] = None
    record["status"] = "MISSING"

    result = PipelineQualityResult.model_validate(record)

    assert result.status == "MISSING"


def test_rejects_status_inconsistent_with_rejection_rate() -> None:
    record = valid_quality_result()
    record["rejection_rate"] = Decimal("0.06")

    with pytest.raises(
        ValidationError,
        match="Quality status must be CRITICAL",
    ):
        PipelineQualityResult.model_validate(record)


def test_rejects_warning_threshold_above_critical_threshold() -> None:
    record = valid_quality_result()
    record["warning_rejection_rate"] = Decimal("0.10")
    record["critical_rejection_rate"] = Decimal("0.05")

    with pytest.raises(
        ValidationError,
        match="warning_rejection_rate cannot exceed",
    ):
        PipelineQualityResult.model_validate(record)