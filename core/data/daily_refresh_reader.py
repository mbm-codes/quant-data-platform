import duckdb

from core.models.daily_refresh import DailyRefreshResult

_DAILY_REFRESH_COLUMNS = [
    "run_id",
    "status",
    "started_at",
    "finished_at",
    "files_discovered",
    "files_processed",
    "files_failed",
    "duplicate_files_skipped",
    "valid_rows",
    "rejected_rows",
    "freshness_status",
    "quality_status",
    "error_message",
]


def load_latest_daily_refresh(
    connection: duckdb.DuckDBPyConnection,
) -> DailyRefreshResult | None:
    """Return the most recently finished daily-refresh result."""
    row = connection.execute(
        """
        SELECT
            run_id,
            status,
            started_at,
            finished_at,
            files_discovered,
            files_processed,
            files_failed,
            duplicate_files_skipped,
            valid_rows,
            rejected_rows,
            freshness_status,
            quality_status,
            error_message
        FROM daily_refreshes
        ORDER BY finished_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        return None

    return DailyRefreshResult.model_validate(
        dict(zip(_DAILY_REFRESH_COLUMNS, row, strict=True))
    )