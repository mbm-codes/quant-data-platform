from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.backtest import backtest_momentum


def make_price(
    trade_date: date,
    close_price: Decimal,
) -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol="RELIANCE",
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file="prices.csv",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def test_backtests_one_complete_momentum_window(
    tmp_path: Path,
) -> None:
    start_date = date(2024, 1, 1)
    close_values = (
        [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)] + [Decimal("133.10")] * 20
    )
    prices = [
        make_price(
            start_date + timedelta(days=index),
            close_price,
        )
        for index, close_price in enumerate(close_values)
    ]
    daily_prices_directory = tmp_path / "daily_prices"
    write_daily_prices_parquet(prices, daily_prices_directory)

    observations = backtest_momentum(
        exchange="NSE",
        symbol="reliance",
        start_date=date(2024, 1, 21),
        end_date=date(2024, 1, 21),
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=daily_prices_directory,
    )

    assert len(observations) == 1
    assert observations[0].symbol == "RELIANCE"
    assert observations[0].as_of_date == date(2024, 1, 21)
    assert observations[0].action == "BUY"
    assert observations[0].forward_return == Decimal("0.10")
