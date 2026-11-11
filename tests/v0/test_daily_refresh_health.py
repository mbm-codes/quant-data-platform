from datetime import UTC, datetime
from typing import Literal

from core.daily_refresh_health import assess_daily_refresh_health
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
        quality_status=(
            "WARNING"
            if status == "COMPLETED_WITH_WARNINGS"
            else "HEALTHY"
        ),
        error_message="Daily refresh failed." if status == "FAILED" else None,
    )


def assess(
    results: list[DailyRefreshResult],
):
    return assess_daily_refresh_health(
        results,
        max_consecutive_failures=2,
        checked_at=datetime(2026, 1, 25, 9, tzinfo=UTC),
    )


def test_returns_missing_when_no_daily_refresh_results_exist() -> None:
    health = assess([])

    assert health.status == "MISSING"
    assert health.consecutive_failure_count == 0


def test_returns_healthy_when_latest_refresh_completed() -> None:
    health = assess(
        [
            make_result(
                run_id="refresh-older",
                status="FAILED",
                finished_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-latest",
                status="COMPLETED",
                finished_at=datetime(2026, 1, 21, 9, tzinfo=UTC),
            ),
        ]
    )

    assert health.status == "HEALTHY"
    assert health.latest_run_id == "refresh-latest"
    assert health.consecutive_failure_count == 0


def test_returns_warning_when_latest_refresh_completed_with_warnings() -> None:
    health = assess(
        [
            make_result(
                run_id="refresh-latest",
                status="COMPLETED_WITH_WARNINGS",
                finished_at=datetime(2026, 1, 21, 9, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-older",
                status="COMPLETED",
                finished_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
            ),
        ]
    )

    assert health.status == "WARNING"
    assert health.latest_run_id == "refresh-latest"
    assert health.consecutive_failure_count == 0


def test_returns_warning_for_one_latest_failure_below_limit() -> None:
    health = assess(
        [
            make_result(
                run_id="refresh-older",
                status="COMPLETED",
                finished_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-latest",
                status="FAILED",
                finished_at=datetime(2026, 1, 21, 9, tzinfo=UTC),
            ),
        ]
    )

    assert health.status == "WARNING"
    assert health.latest_run_id == "refresh-latest"
    assert health.consecutive_failure_count == 1


def test_returns_critical_at_consecutive_failure_limit() -> None:
    health = assess(
        [
            make_result(
                run_id="refresh-first-failure",
                status="FAILED",
                finished_at=datetime(2026, 1, 21, 9, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-older-success",
                status="COMPLETED",
                finished_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-second-failure",
                status="FAILED",
                finished_at=datetime(2026, 1, 22, 9, tzinfo=UTC),
            ),
        ]
    )

    assert health.status == "CRITICAL"
    assert health.latest_run_id == "refresh-second-failure"
    assert health.consecutive_failure_count == 2