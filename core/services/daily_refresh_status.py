from pathlib import Path

from core.data.daily_refresh_reader import load_latest_daily_refresh
from core.data.duckdb import connect, register_daily_refreshes_view
from core.models.daily_refresh import DailyRefreshResult


def get_latest_daily_refresh_status(
    *,
    database_path: Path,
    daily_refreshes_directory: Path,
) -> DailyRefreshResult | None:
    """Return the latest persisted daily-refresh outcome, if one exists."""
    if not any(daily_refreshes_directory.rglob("*.parquet")):
        return None

    connection = connect(database_path)
    try:
        register_daily_refreshes_view(
            connection,
            daily_refreshes_directory,
        )
        return load_latest_daily_refresh(connection)
    finally:
        connection.close()