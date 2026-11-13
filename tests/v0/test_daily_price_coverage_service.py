from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.daily_price_coverage import get_daily_price_coverage


def make_price() -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=date(2024, 1, 2),
        open_price=Decimal("100.00"),
        high_price=Decimal("101.00"),
        low_price=Decimal("99.00"),
        close_price=Decimal("100.00"),
        volume_qty=1_000,
        source_name="test-source",
        source_file="prices.csv.gz",
        source_hash="test-source-hash",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def test_loads_coverage_from_canonical_daily_prices(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    write_daily_prices_parquet(
        [make_price()],
        daily_prices_directory,
    )

    coverage = get_daily_price_coverage(
        exchange="NSE",
        dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=daily_prices_directory,
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert coverage.total_rows == 1
    assert coverage.distinct_symbols == 1
    assert coverage.first_trade_date == date(2024, 1, 2)
    assert coverage.last_trade_date == date(2024, 1, 2)
    assert coverage.duplicate_business_key_count == 0


def test_returns_empty_coverage_when_no_canonical_data_exists(
    tmp_path: Path,
) -> None:
    coverage = get_daily_price_coverage(
        exchange="NSE",
        dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=tmp_path / "daily_prices",
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert coverage.total_rows == 0
    assert coverage.distinct_symbols == 0
    assert coverage.first_trade_date is None
    assert coverage.last_trade_date is None
    assert coverage.duplicate_business_key_count == 0