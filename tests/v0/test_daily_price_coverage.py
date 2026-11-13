from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.data.daily_price_coverage import load_daily_price_coverage
from core.data.duckdb import connect, register_daily_prices_view
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice


def make_price(
    *,
    symbol: str,
    trade_date: date,
    dataset_version: str,
    source_file: str,
) -> DailyPrice:
    close_price = Decimal("100.00")

    return DailyPrice(
        exchange="NSE",
        symbol=symbol,
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + Decimal("1.00"),
        low_price=close_price - Decimal("1.00"),
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file=source_file,
        source_hash=f"hash-{source_file}",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version=dataset_version,
    )


def test_loads_daily_price_coverage_for_dataset_version(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"

    write_daily_prices_parquet(
        [
            make_price(
                symbol="RELIANCE",
                trade_date=date(2024, 1, 2),
                dataset_version="test-v1",
                source_file="one.csv.gz",
            ),
            make_price(
                symbol="TCS",
                trade_date=date(2024, 1, 3),
                dataset_version="test-v1",
                source_file="two.csv.gz",
            ),
            make_price(
                symbol="RELIANCE",
                trade_date=date(2024, 1, 4),
                dataset_version="test-v1",
                source_file="three.csv.gz",
            ),
        ],
        daily_prices_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)
        coverage = load_daily_price_coverage(
            connection,
            exchange="NSE",
            dataset_version="test-v1",
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    finally:
        connection.close()

    assert coverage.total_rows == 3
    assert coverage.distinct_symbols == 2
    assert coverage.first_trade_date == date(2024, 1, 2)
    assert coverage.last_trade_date == date(2024, 1, 4)
    assert coverage.duplicate_business_key_count == 0


def test_counts_duplicate_business_keys(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"

    first_price = make_price(
        symbol="RELIANCE",
        trade_date=date(2024, 1, 2),
        dataset_version="test-v1",
        source_file="first.csv.gz",
    )
    duplicate_price = make_price(
        symbol="RELIANCE",
        trade_date=date(2024, 1, 2),
        dataset_version="test-v1",
        source_file="duplicate.csv.gz",
    )

    write_daily_prices_parquet([first_price], daily_prices_directory)
    write_daily_prices_parquet([duplicate_price], daily_prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)
        coverage = load_daily_price_coverage(
            connection,
            exchange="NSE",
            dataset_version="test-v1",
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    finally:
        connection.close()

    assert coverage.total_rows == 2
    assert coverage.distinct_symbols == 1
    assert coverage.duplicate_business_key_count == 1