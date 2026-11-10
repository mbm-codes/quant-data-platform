from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.data_freshness import get_daily_prices_freshness


def make_price(trade_date: date) -> DailyPrice:
    close_price = Decimal("100")

    return DailyPrice(
        exchange="NSE",
        symbol="ALPHA",
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + Decimal("1"),
        low_price=close_price - Decimal("1"),
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file=f"{trade_date.isoformat()}.csv",
        source_hash="test-source-hash",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def test_reports_fresh_daily_prices(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    write_daily_prices_parquet(
        [make_price(date(2026, 1, 20))],
        daily_prices_directory,
    )

    result = get_daily_prices_freshness(
        as_of_date=date(2026, 1, 20),
        max_age_days=3,
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=daily_prices_directory,
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "FRESH"
    assert result.latest_trade_date == date(2026, 1, 20)


def test_reports_missing_when_no_daily_prices_exist(tmp_path: Path) -> None:
    result = get_daily_prices_freshness(
        as_of_date=date(2026, 1, 20),
        max_age_days=3,
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=tmp_path / "daily_prices",
        checked_at=datetime(2026, 1, 20, 18, tzinfo=UTC),
    )

    assert result.status == "MISSING"