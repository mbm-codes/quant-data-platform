from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from core.data.daily_refresh_ledger import write_daily_refresh_ledger
from core.data.daily_refresh_reader import load_latest_daily_refresh
from core.data.duckdb import connect, register_daily_refreshes_view
from core.models.daily_refresh import DailyRefreshResult
from core.data.daily_refresh_reader import (
    load_daily_refreshes,
    load_latest_daily_refresh,
)

def make_daily_refresh_result(
    *,
    run_id: str = "daily-refresh-001",
    status: Literal[
        "COMPLETED",
        "COMPLETED_WITH_WARNINGS",
        "FAILED",
    ] = "COMPLETED",
    finished_at: datetime = datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
) -> DailyRefreshResult:
    return DailyRefreshResult(
        run_id=run_id,
        status=status,
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=finished_at,
        files_discovered=2,
        files_processed=2,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=3_000,
        rejected_rows=0,
        freshness_status="FRESH",
        quality_status="HEALTHY",
        error_message=(
            "Daily refresh failed."
            if status == "FAILED"
            else None
        ),
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

def test_loads_daily_refresh_history_in_finish_time_order(
    tmp_path: Path,
) -> None:
    daily_refreshes_directory = tmp_path / "daily_refreshes"

    write_daily_refresh_ledger(
        [
            make_daily_refresh_result(
                run_id="daily-refresh-002",
                status="COMPLETED_WITH_WARNINGS",
                finished_at=datetime(2026, 1, 21, 9, 2, tzinfo=UTC),
            ),
            make_daily_refresh_result(
                run_id="daily-refresh-001",
                finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
            ),
        ],
        daily_refreshes_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_refreshes_view(
            connection,
            daily_refreshes_directory,
        )
        results = load_daily_refreshes(connection)
    finally:
        connection.close()

    assert [result.run_id for result in results] == [
        "daily-refresh-001",
        "daily-refresh-002",
    ]
    assert results[1].status == "COMPLETED_WITH_WARNINGS"