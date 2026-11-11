from datetime import UTC, datetime
from pathlib import Path

from core.data.daily_refresh_ledger import write_daily_refresh_ledger
from core.data.daily_refresh_reader import load_latest_daily_refresh
from core.data.duckdb import connect, register_daily_refreshes_view
from core.models.daily_refresh import DailyRefreshResult


def make_daily_refresh_result() -> DailyRefreshResult:
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


def test_loads_latest_daily_refresh_result(tmp_path: Path) -> None:
    daily_refreshes_directory = tmp_path / "daily_refreshes"
    write_daily_refresh_ledger(
        [make_daily_refresh_result()],
        daily_refreshes_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_refreshes_view(
            connection,
            daily_refreshes_directory,
        )
        result = load_latest_daily_refresh(connection)
    finally:
        connection.close()

    assert result is not None
    assert result.run_id == "daily-refresh-001"
    assert result.status == "COMPLETED"
    assert result.freshness_status == "FRESH"
    assert result.quality_status == "HEALTHY"