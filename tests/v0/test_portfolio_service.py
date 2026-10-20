# tests/v0/test_portfolio_service.py

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.config.settings import get_settings
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.portfolio import get_portfolio_positions, value_portfolio


def test_get_portfolio_positions_returns_active_position(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,4,2520.00,20.00\n",
        encoding="utf-8",
    )

    positions = get_portfolio_positions(
        source=source_path,
        run_id="test-run-1",
        ingested_at=datetime.now(UTC),
    )

    assert positions[0].account_id == "main"
    assert positions[0].exchange == "NSE"
    assert positions[0].symbol == "RELIANCE"
    assert positions[0].quantity == Decimal(6)
    assert positions[0].currency == "INR"


def test_value_portfolio_returns_current_valuation(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,4,2520.00,20.00\n",
        encoding="utf-8",
    )

    daily_prices_directory = tmp_path / "daily_prices"

    price = DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=date(2024, 3, 1),
        open_price=Decimal("2900.00"),
        high_price=Decimal("2930.00"),
        low_price=Decimal("2890.00"),
        close_price=Decimal("2915.40"),
        volume_qty=1_000,
        source_name="test_bhavcopy",
        source_file="prices.csv.gz",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 3, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )

    write_daily_prices_parquet([price], daily_prices_directory)

    valuations = value_portfolio(
        source=source_path,
        run_id="test-run-2",
        ingested_at=datetime(2025, 2, 2, tzinfo=UTC),
        as_of=date(2024, 3, 1),
        max_price_age_days=7,
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=daily_prices_directory,
    )

    assert valuations[0].account_id == "main"
    assert valuations[0].exchange == "NSE"
    assert valuations[0].symbol == "RELIANCE"
    assert valuations[0].quantity == Decimal(6)
    assert valuations[0].currency == "INR"
    assert valuations[0].price_date == date(2024, 3, 1)
    assert valuations[0].close_price == Decimal("2915.40")
    assert valuations[0].market_value == Decimal("17492.40")


def test_value_portfolio_returns_empty_list_without_positions(tmp_path: Path) -> None:
    source_path = tmp_path / "portfolio_transactions.csv"
    source_path.write_text(
        "transaction_id,account_id,symbol,trade_date,side,"
        "quantity,unit_price,fees\n"
        "txn-001,main,reliance,2024-01-02,buy,10,2510.00,20.00\n"
        "txn-002,main,reliance,2024-01-03,sell,10,2520.00,20.00\n",
        encoding="utf-8",
    )
    settings = get_settings()
    valuations = value_portfolio(
        source=source_path,
        run_id="test-run-2",
        ingested_at=datetime.now(UTC),
        as_of=date(2024, 2, 2),
        max_price_age_days=7,
        database_path=settings.database_path,
        daily_prices_directory=settings.data_dir / "daily_prices",
    )

    assert valuations == []
