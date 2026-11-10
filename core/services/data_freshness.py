from datetime import date, datetime
from pathlib import Path

from core.data.duckdb import connect, register_daily_prices_view
from core.data.price_history import load_latest_trade_date
from core.data_freshness import assess_data_freshness
from core.models.data_freshness import DataFreshnessResult


def get_daily_prices_freshness(
    *,
    as_of_date: date,
    max_age_days: int,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    checked_at: datetime,
) -> DataFreshnessResult:
    """Assess freshness of canonical NSE daily-price data."""
    if not any(daily_prices_directory.glob("trade_year=*/*.parquet")):
        return assess_data_freshness(
            dataset_name="daily_prices",
            as_of_date=as_of_date,
            latest_trade_date=None,
            max_age_days=max_age_days,
            checked_at=checked_at,
        )

    connection = connect(database_path)

    try:
        register_daily_prices_view(
            connection,
            daily_prices_directory,
        )
        latest_trade_date = load_latest_trade_date(
            connection,
            exchange="NSE",
            dataset_version=input_dataset_version,
        )
    finally:
        connection.close()

    return assess_data_freshness(
        dataset_name="daily_prices",
        as_of_date=as_of_date,
        latest_trade_date=latest_trade_date,
        max_age_days=max_age_days,
        checked_at=checked_at,
    )