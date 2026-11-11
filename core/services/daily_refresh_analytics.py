from datetime import datetime
from pathlib import Path

from core.daily_refresh_analytics import summarize_daily_refreshes
from core.data.daily_refresh_reader import load_daily_refreshes
from core.data.duckdb import connect, register_daily_refreshes_view
from core.models.daily_refresh_analytics import DailyRefreshSummary


def get_daily_refresh_summary(
    *,
    database_path: Path,
    daily_refreshes_directory: Path,
    generated_at: datetime,
) -> DailyRefreshSummary | None:
    """Return reliability metrics across all persisted daily refreshes."""
    if not any(daily_refreshes_directory.rglob("*.parquet")):
        return None

    connection = connect(database_path)
    try:
        register_daily_refreshes_view(
            connection,
            daily_refreshes_directory,
        )
        results = load_daily_refreshes(connection)
    finally:
        connection.close()

    return summarize_daily_refreshes(
        results,
        generated_at=generated_at,
    )