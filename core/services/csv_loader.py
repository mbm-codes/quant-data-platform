# core/services/csv_loader.py

from pathlib import Path

from core.data.daily_prices import load_daily_prices_csv
from core.data.parquet import write_daily_prices_parquet


def load_csv(
    source: Path,
    daily_prices_directory: Path,
) -> list[Path]:
    """Load canonical daily-price CSV data into Parquet."""

    records = load_daily_prices_csv(source)

    written_paths = write_daily_prices_parquet(
        records,
        daily_prices_directory,
    )

    return written_paths
