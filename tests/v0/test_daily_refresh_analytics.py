from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from core.daily_refresh_analytics import summarize_daily_refreshes
from core.models.daily_refresh import DailyRefreshResult


def make_result(
    *,
    run_id: str,
    status: Literal[
        "COMPLETED",
        "COMPLETED_WITH_WARNINGS",
        "FAILED",
    ],
    finished_at: datetime,
) -> DailyRefreshResult:
    return DailyRefreshResult(
        run_id=run_id,
        status=status,
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=finished_at,
        files_discovered=1,
        files_processed=1,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=1_000,
        rejected_rows=0,
        freshness_status="FRESH",
        quality_status="HEALTHY",
        error_message=(
            "Refresh quality gate failed."
            if status == "FAILED"
            else None
        ),
    )


def test_summarizes_daily_refresh_outcomes() -> None:
    summary = summarize_daily_refreshes(
        [
            make_result(
                run_id="refresh-001",
                status="COMPLETED",
                finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-002",
                status="COMPLETED_WITH_WARNINGS",
                finished_at=datetime(2026, 1, 21, 9, 2, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-003",
                status="FAILED",
                finished_at=datetime(2026, 1, 22, 9, 2, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-004",
                status="FAILED",
                finished_at=datetime(2026, 1, 23, 9, 2, tzinfo=UTC),
            ),
        ],
        generated_at=datetime(2026, 1, 24, 9, tzinfo=UTC),
    )

    assert summary.total_refreshes == 4
    assert summary.completed_count == 1
    assert summary.completed_with_warnings_count == 1
    assert summary.failed_count == 2
    assert summary.success_rate == Decimal("0.5")
    assert summary.latest_successful_finished_at == datetime(
        2026,
        1,
        21,
        9,
        2,
        tzinfo=UTC,
    )


def test_summarizes_empty_daily_refresh_history() -> None:
    summary = summarize_daily_refreshes(
        [],
        generated_at=datetime(2026, 1, 24, 9, tzinfo=UTC),
    )

    assert summary.total_refreshes == 0
    assert summary.success_rate == Decimal(0)
    assert summary.latest_successful_finished_at is None