from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from core.data.daily_refresh_ledger import write_daily_refresh_ledger
from core.models.daily_refresh import DailyRefreshResult
from core.services.daily_refresh_analytics import (
    get_daily_refresh_summary,
)


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
        error_message="Refresh failed." if status == "FAILED" else None,
    )


def test_summarizes_persisted_daily_refresh_history(
    tmp_path: Path,
) -> None:
    daily_refreshes_directory = tmp_path / "daily_refreshes"
    write_daily_refresh_ledger(
        [
            make_result(
                run_id="refresh-001",
                status="COMPLETED",
                finished_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
            ),
            make_result(
                run_id="refresh-002",
                status="FAILED",
                finished_at=datetime(2026, 1, 21, 9, tzinfo=UTC),
            ),
        ],
        daily_refreshes_directory,
    )

    summary = get_daily_refresh_summary(
        database_path=tmp_path / "qdp.duckdb",
        daily_refreshes_directory=daily_refreshes_directory,
        generated_at=datetime(2026, 1, 22, 9, tzinfo=UTC),
    )

    assert summary is not None
    assert summary.total_refreshes == 2
    assert summary.completed_count == 1
    assert summary.failed_count == 1
    assert summary.success_rate == 0.5


def test_returns_none_when_no_daily_refresh_history_exists(
    tmp_path: Path,
) -> None:
    summary = get_daily_refresh_summary(
        database_path=tmp_path / "qdp.duckdb",
        daily_refreshes_directory=tmp_path / "daily_refreshes",
        generated_at=datetime(2026, 1, 22, 9, tzinfo=UTC),
    )

    assert summary is None