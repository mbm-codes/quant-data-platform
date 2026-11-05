# tests/v0/test_momentum_backtest.py

from datetime import date, timedelta
from decimal import Decimal

import pytest

from core.strategies.momentum import (
    build_momentum_backtest_observation,
    build_momentum_backtest_observations,
)


def test_builds_completed_momentum_backtest_observation() -> None:
    start_date = date(2024, 1, 1)
    historical_closes = (
        [(start_date, Decimal(100))]
        + [
            (
                start_date + timedelta(days=index),
                Decimal(101),
            )
            for index in range(1, 20)
        ]
        + [(start_date + timedelta(days=20), Decimal(121))]
    )
    future_closes = [
        (
            start_date + timedelta(days=index),
            Decimal("133.10"),
        )
        for index in range(21, 41)
    ]

    observation = build_momentum_backtest_observation(
        exchange="NSE",
        symbol="reliance",
        input_dataset_version="test-v1",
        historical_closes=historical_closes,
        future_closes=future_closes,
    )

    assert observation.symbol == "RELIANCE"
    assert observation.as_of_date == date(2024, 1, 21)
    assert observation.reference_price == Decimal(121)
    assert observation.momentum_return == Decimal("0.21")
    assert observation.score == Decimal(1)
    assert observation.action == "BUY"
    assert observation.future_price_date == date(2024, 2, 10)
    assert observation.future_close_price == Decimal("133.10")
    assert observation.forward_return == Decimal("0.10")


@pytest.mark.parametrize(
    ("historical_count", "future_count", "error_message"),
    [
        (20, 20, "exactly 21 historical"),
        (21, 19, "exactly 20 future"),
    ],
)
def test_rejects_incomplete_backtest_windows(
    historical_count: int,
    future_count: int,
    error_message: str,
) -> None:
    start_date = date(2024, 1, 1)
    historical_closes = [
        (
            start_date + timedelta(days=index),
            Decimal(110),
        )
        for index in range(historical_count)
    ]
    future_closes = [
        (
            start_date + timedelta(days=historical_count + index),
            Decimal(
                110,
            ),
        )
        for index in range(future_count)
    ]

    with pytest.raises(ValueError, match=error_message):
        build_momentum_backtest_observation(
            exchange="NSE",
            symbol="RELIANCE",
            input_dataset_version="test-v1",
            historical_closes=historical_closes,
            future_closes=future_closes,
        )


def test_builds_observations_for_eligible_as_of_dates() -> None:
    start_date = date(2024, 1, 1)
    closes = [
        (
            start_date + timedelta(days=index),
            Decimal(100) + Decimal(index),
        )
        for index in range(45)
    ]

    observations = build_momentum_backtest_observations(
        exchange="NSE",
        symbol="reliance",
        input_dataset_version="test-v1",
        closes=closes,
        start_date=date(2024, 1, 21),
        end_date=date(2024, 1, 23),
    )

    assert len(observations) == 3
    assert [observation.as_of_date for observation in observations] == [
        date(2024, 1, 21),
        date(2024, 1, 22),
        date(2024, 1, 23),
    ]
    assert all(
        observation.future_price_date > observation.as_of_date
        for observation in observations
    )


def test_returns_no_observations_without_complete_windows() -> None:
    start_date = date(2024, 1, 1)
    closes = [
        (
            start_date + timedelta(days=index),
            Decimal(100),
        )
        for index in range(40)
    ]

    observations = build_momentum_backtest_observations(
        exchange="NSE",
        symbol="RELIANCE",
        input_dataset_version="test-v1",
        closes=closes,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 9),
    )

    assert observations == []


def test_rejects_non_ascending_close_dates() -> None:
    closes = [
        (date(2024, 1, 2), Decimal(100)),
        (date(2024, 1, 1), Decimal(101)),
    ]

    with pytest.raises(ValueError, match="unique dates in ascending order"):
        build_momentum_backtest_observations(
            exchange="NSE",
            symbol="RELIANCE",
            input_dataset_version="test-v1",
            closes=closes,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
        )


def test_samples_every_second_eligible_backtest_observation() -> None:
    start_date = date(2024, 1, 1)
    closes = [
        (
            start_date + timedelta(days=index),
            Decimal(100) + Decimal(index),
        )
        for index in range(45)
    ]

    observations = build_momentum_backtest_observations(
        exchange="NSE",
        symbol="RELIANCE",
        input_dataset_version="test-v1",
        closes=closes,
        start_date=date(2024, 1, 21),
        end_date=date(2024, 1, 25),
        step_observations=2,
    )
    assert [observation.as_of_date for observation in observations] == [
        date(2024, 1, 21),
        date(2024, 1, 23),
        date(2024, 1, 25),
    ]


def test_rejects_non_positive_backtest_step() -> None:
    with pytest.raises(
        ValueError,
        match="step_observations must be greater than zero",
    ):
        build_momentum_backtest_observations(
            exchange="NSE",
            symbol="RELIANCE",
            input_dataset_version="test-v1",
            closes=[],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
            step_observations=0,
        )
