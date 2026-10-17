from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.duckdb import connect, register_daily_prices_view
from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.models.portfolio import PortfolioPosition
from core.valuation import value_positions


def make_price(trade_date: date, close_price: str) -> DailyPrice:
    return DailyPrice.model_validate(
        {
            "exchange": "NSE",
            "symbol": "RELIANCE",
            "trade_date": trade_date,
            "open_price": "2500.00",
            "high_price": "2530.00",
            "low_price": "2490.00",
            "close_price": close_price,
            "volume_qty": 1_000,
            "source_name": "nse_bhavcopy",
            "source_file": "bhavcopy.csv.gz",
            "ingested_at": datetime.now(UTC),
            "run_id": "test-run",
            "dataset_version": "test-v1",
        }
    )


def test_values_position_using_latest_close_on_or_before_as_of(
    tmp_path: Path,
) -> None:
    parquet_directory = tmp_path / "daily_prices"
    write_daily_prices_parquet(
        [
            make_price(date(2024, 1, 2), "2510.00"),
            make_price(date(2024, 1, 3), "2520.00"),
        ],
        parquet_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    register_daily_prices_view(connection, parquet_directory)

    positions = [
        PortfolioPosition(account_id="main", symbol="RELIANCE", quantity=Decimal(6))
    ]

    valuations = value_positions(
        connection,
        positions,
        as_of=date(2024, 1, 5),
    )

    connection.close()

    assert len(valuations) == 1
    assert valuations[0].price_date == date(2024, 1, 3)
    assert valuations[0].close_price == Decimal("2520.00")
    assert valuations[0].market_value == Decimal("15120.00")


def test_rejects_position_without_available_price(tmp_path: Path) -> None:
    parquet_directory = tmp_path / "daily_prices"
    write_daily_prices_parquet(
        [make_price(date(2024, 1, 2), "2510.00")],
        parquet_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    register_daily_prices_view(connection, parquet_directory)

    positions = [
        PortfolioPosition(
            account_id="main",
            symbol="TCS",
            quantity=Decimal(1),
        )
    ]

    with pytest.raises(ValueError, match="No close price"):
        value_positions(
            connection,
            positions,
            as_of=date(2024, 1, 5),
        )

    connection.close()
