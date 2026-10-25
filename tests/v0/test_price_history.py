from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from core.data.duckdb import connect, register_daily_prices_view
from core.data.parquet import write_daily_prices_parquet
from core.data.price_history import load_recent_closes
from core.models.daily_prices import DailyPrice


def make_price(trade_date: date, close_price: Decimal) -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + Decimal(1),
        low_price=close_price - Decimal(1),
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file="prices.csv",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def write_price_history(output_directory: Path) -> list[DailyPrice]:
    start_date = date(2024, 1, 1)

    prices = [
        make_price(
            trade_date=start_date + timedelta(days=index),
            close_price=Decimal(100) + Decimal(index),
        )
        for index in range(22)
    ]

    write_daily_prices_parquet(prices, output_directory)

    return prices


def test_load_recent_closes_returns_latest_twenty_one_in_order(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    write_price_history(daily_prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)

        closes = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="reliance",
            as_of=None,
        )
    finally:
        connection.close()

    assert len(closes) == 21
    assert closes[0] == (date(2024, 1, 2), Decimal("101.00000000"))
    assert closes[-1] == (date(2024, 1, 22), Decimal("121.00000000"))


def test_load_recent_closes_respects_as_of_date(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    write_price_history(daily_prices_directory)

    connection = connect(tmp_path / "qdp.duckddb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)

        closes = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            as_of=date(2024, 1, 15),
        )
    finally:
        connection.close()

    assert len(closes) == 15
    assert closes[0] == (date(2024, 1, 1), Decimal("100.00000000"))
    assert closes[-1] == (date(2024, 1, 15), Decimal("114.00000000"))
