from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from core.ranking_backtest import (
    build_momentum_ranking_backtest_observation,
)


def build_symbol_history(
    *,
    ranking_date: date,
    reference_price: Decimal,
    future_price: Decimal,
) -> list[tuple[date, Decimal]]:
    historical_closes = [
        (
            ranking_date - timedelta(days=20 - index),
            Decimal(100),
        )
        for index in range(20)
    ]
    historical_closes.append((ranking_date, reference_price))

    future_closes = [
        (
            ranking_date + timedelta(days=index),
            future_price,
        )
        for index in range(1, 21)
    ]

    return historical_closes + future_closes


def test_builds_one_ranking_backtest_observation() -> None:
    ranking_date = date(2024, 1, 21)
    evaluation_as_of_date = date(2024, 2, 10)

    closes_by_symbol = {
        # +10% momentum, then +10% forward return.
        "ALPHA": build_symbol_history(
            ranking_date=ranking_date,
            reference_price=Decimal(110),
            future_price=Decimal(121),
        ),
        # -10% momentum, then -10% forward return.
        "BETA": build_symbol_history(
            ranking_date=ranking_date,
            reference_price=Decimal(90),
            future_price=Decimal(81),
        ),
    }

    observation = build_momentum_ranking_backtest_observation(
        closes_by_symbol,
        ranking_date=ranking_date,
        evaluation_as_of_date=evaluation_as_of_date,
        bucket_size=1,
        input_dataset_version="test-v1",
        universe_hash="a" * 64,
        generated_at=datetime(2024, 2, 11, tzinfo=UTC),
    )

    assert observation.ranking_date == ranking_date
    assert observation.top_rank_start == 1
    assert observation.top_rank_end == 1
    assert observation.bottom_rank_start == 2
    assert observation.bottom_rank_end == 2
    assert observation.top_average_forward_return == Decimal("0.10")
    assert observation.bottom_average_forward_return == Decimal("-0.10")
    assert observation.rank_spread == Decimal("0.20")
    assert observation.top_positive_return_rate == Decimal(1)
    assert observation.bottom_positive_return_rate == Decimal(0)


def test_rejects_selected_candidate_without_twenty_future_observations() -> None:
    ranking_date = date(2024, 1, 21)

    alpha_history = build_symbol_history(
        ranking_date=ranking_date,
        reference_price=Decimal(110),
        future_price=Decimal(121),
    )
    beta_history = build_symbol_history(
        ranking_date=ranking_date,
        reference_price=Decimal(90),
        future_price=Decimal(81),
    )

    closes_by_symbol = {
        # Remove one future close, leaving ALPHA with only 19.
        "ALPHA": alpha_history[:-1],
        "BETA": beta_history,
    }

    with pytest.raises(
        ValueError,
        match="ALPHA does not have 20 future observations",
    ):
        build_momentum_ranking_backtest_observation(
            closes_by_symbol,
            ranking_date=ranking_date,
            evaluation_as_of_date=date(2024, 2, 10),
            bucket_size=1,
            input_dataset_version="test-v1",
            universe_hash="a" * 64,
            generated_at=datetime(2024, 2, 11, tzinfo=UTC),
        )
