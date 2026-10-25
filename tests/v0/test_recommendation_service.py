# tests/v0/test_recommendation_service.py
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.recommendation import preview_momentum_recommendation


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


def write_price_history(
    output_directory: Path, close_values: list[Decimal]
) -> list[DailyPrice]:
    start_date = date(2024, 1, 1)

    prices = [
        make_price(
            trade_date=start_date + timedelta(days=index),
            close_price=close_price,
        )
        for index, close_price in enumerate(close_values)
    ]

    write_daily_prices_parquet(prices, output_directory)

    return prices


def test_momentum_recommendation(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    database_path = tmp_path / "qdp.duckdb"
    close_values = [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)]
    write_price_history(daily_prices_directory, close_values)

    recommendation = preview_momentum_recommendation(
        account_id="test-account",
        as_of=None,
        run_id="test-run",
        generated_at=datetime(2024, 1, 22, tzinfo=UTC),
        input_dataset_version="test-v1",
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
        symbol="reliance",
    )

    assert recommendation.recommendation_id == (
        "baseline_momentum:v1:test-account:RELIANCE:2024-01-21"
    )
    assert recommendation.symbol == "RELIANCE"
    assert recommendation.action == "BUY"
    assert recommendation.as_of_date == date(2024, 1, 21)
    assert recommendation.price_date == date(2024, 1, 21)
    assert recommendation.reference_price == Decimal("121.00000000")
    assert recommendation.score == Decimal(1)
    assert recommendation.strategy_name == "baseline_momentum"
    assert recommendation.strategy_version == "v1"
    assert "21.00%" in recommendation.rationale


def test_momentum_recommendation_22_days(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    database_path = tmp_path / "qdp.duckdb"
    # 22-price as_of test
    close_price = (
        [Decimal(100)]
        + [Decimal(101)] * 19
        + [
            Decimal(121),
            Decimal(122),
        ]
    )
    write_price_history(daily_prices_directory, close_price)

    recommendation = preview_momentum_recommendation(
        account_id="test-account",
        as_of=date(2024, 1, 21),
        run_id="test-run",
        generated_at=datetime(2024, 1, 22, tzinfo=UTC),
        input_dataset_version="test-v1",
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
        symbol="reliance",
    )

    assert recommendation.recommendation_id == (
        "baseline_momentum:v1:test-account:RELIANCE:2024-01-21"
    )
    assert recommendation.symbol == "RELIANCE"
    assert recommendation.action == "BUY"
    assert recommendation.as_of_date == date(2024, 1, 21)
    assert recommendation.price_date == date(2024, 1, 21)
    assert recommendation.reference_price == Decimal("121.00000000")
    assert recommendation.score == Decimal(1)
    assert recommendation.strategy_name == "baseline_momentum"
    assert recommendation.strategy_version == "v1"
    assert "21.00%" in recommendation.rationale


def test_momentum_recommendation_insufficient_days(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    database_path = tmp_path / "qdp.duckdb"
    # 22-price as_of test
    close_price = (
        [Decimal(100)]
        + [Decimal(101)] * 18
        + [
            Decimal(120),
        ]
    )
    write_price_history(daily_prices_directory, close_price)

    with pytest.raises(ValueError, match="21"):
        preview_momentum_recommendation(
            account_id="test-account",
            as_of=date(2024, 1, 21),
            run_id="test-run",
            generated_at=datetime(2024, 1, 22, tzinfo=UTC),
            input_dataset_version="test-v1",
            database_path=database_path,
            daily_prices_directory=daily_prices_directory,
            symbol="reliance",
        )


def test_recommendation_accepts_recent_price(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    close_values = [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)]
    write_price_history(daily_prices_directory, close_values)

    recommendation = preview_momentum_recommendation(
        account_id="test-account",
        symbol="RELIANCE",
        as_of=date(2024, 1, 22),
        run_id="test-run",
        generated_at=datetime(2024, 1, 22, tzinfo=UTC),
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=daily_prices_directory,
    )

    assert recommendation.price_date == date(2024, 1, 21)
    assert recommendation.action == "BUY"


@pytest.mark.parametrize("as_of", [date(2024, 2, 1), None])
def test_recommendation_rejects_stale_price(
    tmp_path: Path,
    as_of: date | None,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    close_values = [Decimal(100)] + [Decimal(101)] * 19 + [Decimal(121)]

    write_price_history(daily_prices_directory, close_values)

    with pytest.raises(ValueError, match="Stale close price"):
        preview_momentum_recommendation(
            account_id="test-account",
            symbol="RELIANCE",
            as_of=as_of,
            run_id="test-run",
            generated_at=datetime(2024, 2, 1, tzinfo=UTC),
            input_dataset_version="test-v1",
            database_path=tmp_path / "qdp.duckdb",
            daily_prices_directory=daily_prices_directory,
        )
