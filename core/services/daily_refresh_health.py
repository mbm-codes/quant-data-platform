from datetime import datetime
from pathlib import Path

from core.daily_refresh_health import assess_daily_refresh_health
from core.data.daily_refresh_reader import load_daily_refreshes
from core.data.duckdb import connect, register_daily_refreshes_view
from core.models.daily_refresh_health import DailyRefreshHealth


def get_daily_refresh_health(
    *,
    database_path: Path,
    daily_refreshes_directory: Path,
    max_consecutive_failures: int,
    checked_at: datetime,
) -> DailyRefreshHealth:
    """Return the current actionable health state for daily refreshes."""
    if not any(daily_refreshes_directory.rglob("*.parquet")):
        return assess_daily_refresh_health(
            [],
            max_consecutive_failures=max_consecutive_failures,
            checked_at=checked_at,
        )

    connection = connect(database_path)
    try:
        register_daily_refreshes_view(
            connection,
            daily_refreshes_directory,
        )
        results = load_daily_refreshes(connection)
    finally:
        connection.close()

    return assess_daily_refresh_health(
        results,
        max_consecutive_failures=max_consecutive_failures,
        checked_at=checked_at,
    )