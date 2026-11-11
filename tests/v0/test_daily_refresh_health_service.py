from datetime import UTC, datetime
from pathlib import Path

from core.data.daily_refresh_ledger import write_daily_refresh_ledger
from core.models.daily_refresh import DailyRefreshResult
from core.services.daily_refresh_health import get_daily_refresh_health


def make_completed_result() -> DailyRefreshResult:
    return DailyRefreshResult(
        run_id="daily-refresh-001",
        status="COMPLETED",
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        files_discovered=2,
        files_processed=2,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=3_000,
        rejected_rows=0,
        freshness_status="FRESH",
        quality_status="HEALTHY",
    )


def test_returns_health_from_persisted_daily_refresh_history(
    tmp_path: Path,
) -> None:
    daily_refreshes_directory = tmp_path / "daily_refreshes"
    write_daily_refresh_ledger(
        [make_completed_result()],
        daily_refreshes_directory,
    )

    health = get_daily_refresh_health(
        database_path=tmp_path / "qdp.duckdb",
        daily_refreshes_directory=daily_refreshes_directory,
        max_consecutive_failures=2,
        checked_at=datetime(2026, 1, 20, 9, 3, tzinfo=UTC),
    )

    assert health.status == "HEALTHY"
    assert health.latest_run_id == "daily-refresh-001"
    assert health.consecutive_failure_count == 0


def test_returns_missing_when_no_daily_refresh_history_exists(
    tmp_path: Path,
) -> None:
    health = get_daily_refresh_health(
        database_path=tmp_path / "qdp.duckdb",
        daily_refreshes_directory=tmp_path / "daily_refreshes",
        max_consecutive_failures=2,
        checked_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
    )

    assert health.status == "MISSING"
    assert health.latest_run_id is None
    assert health.consecutive_failure_count == 0