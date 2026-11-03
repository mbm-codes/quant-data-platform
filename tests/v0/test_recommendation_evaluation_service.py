from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.models.recommendations import PortfolioRecommendation
from core.services.recommendation_evaluation import evaluate_recommendation


def make_price(trade_date: date, close_price: Decimal) -> DailyPrice:
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
        source_file=f"prices-{trade_date.isoformat()}.csv",
        source_hash=f"source-hash-{trade_date.isoformat()}",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def make_recommendation() -> PortfolioRecommendation:
    return PortfolioRecommendation(
        recommendation_id="rec-001",
        account_id="main",
        exchange="NSE",
        symbol="RELIANCE",
        action="BUY",
        as_of_date=date(2024, 1, 1),
        price_date=date(2024, 1, 1),
        reference_price=Decimal(100),
        score=Decimal("0.75"),
        rationale="Test recommendation.",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        generated_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        input_dataset_version="test-v1",
    )


def test_evaluates_recommendation_after_twenty_future_prices(
    tmp_path: Path,
) -> None:
    recommendation = make_recommendation()
    prices_directory = tmp_path / "daily_prices"

    prices = [
        make_price(
            trade_date=date(2024, 1, 1) + timedelta(days=index),
            close_price=Decimal(101) + Decimal(index - 1),
        )
        for index in range(1, 21)
    ]
    prices[-1] = make_price(date(2024, 1, 21), Decimal(110))

    write_daily_prices_parquet(prices, prices_directory)

    evaluation = evaluate_recommendation(
        recommendation,
        evaluation_as_of_date=date(2024, 1, 21),
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=prices_directory,
    )

    assert evaluation.recommendation_id == "rec-001"
    assert evaluation.status == "COMPLETED"
    assert evaluation.future_observations_found == 20
    assert evaluation.future_price_date == date(2024, 1, 21)
    assert evaluation.future_close_price == Decimal(110)
    assert evaluation.forward_return == Decimal("0.10")


def test_returns_pending_when_fewer_than_twenty_future_prices_exist(
    tmp_path: Path,
) -> None:
    recommendation = make_recommendation()
    prices_directory = tmp_path / "daily_prices"

    prices = [
        make_price(
            trade_date=date(2024, 1, 1) + timedelta(days=index),
            close_price=Decimal(100) + Decimal(index),
        )
        for index in range(1, 20)
    ]
    write_daily_prices_parquet(prices, prices_directory)

    evaluation = evaluate_recommendation(
        recommendation,
        evaluation_as_of_date=date(2024, 1, 20),
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=prices_directory,
    )

    assert evaluation.status == "PENDING"
    assert evaluation.future_observations_found == 19
    assert evaluation.future_price_date is None
    assert evaluation.future_close_price is None
    assert evaluation.forward_return is None


def test_uses_recommendation_dataset_version_when_evaluating(
    tmp_path: Path,
) -> None:
    recommendation = make_recommendation()
    prices_directory = tmp_path / "daily_prices"

    prices = [
        make_price(
            trade_date=date(2024, 1, 1) + timedelta(days=index),
            close_price=Decimal(100) + Decimal(index),
        ).model_copy(update={"dataset_version": "other-v1"})
        for index in range(1, 21)
    ]
    write_daily_prices_parquet(prices, prices_directory)

    evaluation = evaluate_recommendation(
        recommendation,
        evaluation_as_of_date=date(2024, 1, 21),
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=prices_directory,
    )

    assert evaluation.status == "PENDING"
    assert evaluation.future_observations_found == 0
    assert evaluation.future_price_date is None
    assert evaluation.future_close_price is None
    assert evaluation.forward_return is None


def test_rejects_evaluation_before_recommendation_price_date(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="cannot be before the recommendation price date",
    ):
        evaluate_recommendation(
            make_recommendation(),
            evaluation_as_of_date=date(2023, 12, 31),
            database_path=tmp_path / "qdp.duckdb",
            daily_prices_directory=tmp_path / "daily_prices",
        )
