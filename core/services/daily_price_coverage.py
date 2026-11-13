from datetime import datetime
from pathlib import Path
from typing import Literal

from core.data.daily_price_coverage import load_daily_price_coverage
from core.data.duckdb import connect, register_daily_prices_view
from core.models.daily_price_coverage import DailyPriceCoverage


def get_daily_price_coverage(
    *,
    exchange: Literal["NSE"],
    dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    generated_at: datetime,
) -> DailyPriceCoverage:
    """Return canonical daily-price coverage for one dataset version."""
    if not any(daily_prices_directory.rglob("*.parquet")):
        return DailyPriceCoverage(
            exchange=exchange,
            dataset_version=dataset_version,
            total_rows=0,
            distinct_symbols=0,
            duplicate_business_key_count=0,
            generated_at=generated_at,
        )

    connection = connect(database_path)
    try:
        register_daily_prices_view(connection, daily_prices_directory)
        return load_daily_price_coverage(
            connection,
            exchange=exchange,
            dataset_version=dataset_version,
            generated_at=generated_at,
        )
    finally:
        connection.close()