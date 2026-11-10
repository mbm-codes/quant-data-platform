from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from core.data_quality import assess_pipeline_quality
from core.models.pipeline_run import PipelineRun


def make_pipeline_run(
    *,
    status: Literal[
        "RUNNING",
        "COMPLETED",
        "COMPLETED_WITH_WARNINGS",
        "FAILED",
    ] = "COMPLETED",
    valid_rows: int = 1_000,
    rejected_rows: int = 0,
) -> PipelineRun:
    return PipelineRun(
        run_id="stage-bhavcopy-001",
        pipeline_name="stage_bhavcopy",
        input_dataset_version="nse-bhavcopy-v1",
        status=status,
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=(
            datetime(2026, 1, 20, 9, 2, tzinfo=UTC)
            if status != "RUNNING"
            else None
        ),
        files_discovered=1,
        files_processed=1,
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        error_message=(
            "Source file failed."
            if status == "FAILED"
            else None
        ),
    )


def test_assesses_healthy_completed_run() -> None:
    result = assess_pipeline_quality(
        pipeline_run=make_pipeline_run(),
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "HEALTHY"
    assert result.rejection_rate == Decimal("0")


def test_assesses_warning_for_rejection_rate_above_threshold() -> None:
    result = assess_pipeline_quality(
        pipeline_run=make_pipeline_run(
            valid_rows=980,
            rejected_rows=20,
        ),
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "WARNING"
    assert result.rejection_rate == Decimal("0.02")


def test_assesses_critical_for_failed_pipeline_run() -> None:
    result = assess_pipeline_quality(
        pipeline_run=make_pipeline_run(status="FAILED"),
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "CRITICAL"


def test_assesses_missing_when_no_pipeline_run_exists() -> None:
    result = assess_pipeline_quality(
        pipeline_run=None,
        pipeline_name="stage_bhavcopy",
        warning_rejection_rate=Decimal("0.01"),
        critical_rejection_rate=Decimal("0.05"),
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "MISSING"