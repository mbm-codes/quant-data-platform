# core/strategies/momentum.py

from datetime import date
from decimal import Decimal
from itertools import pairwise
from typing import Literal

from core.models.backtest import (
    MomentumBacktestObservation,
)
from core.recommendation_evaluation import calculate_forward_return


def select_momentum_action(momentum_return: Decimal) -> Literal["BUY", "SELL", "HOLD"]:
    """Select the baseline action from a 20-observation momentum return."""
    if momentum_return <= Decimal("-0.02"):
        return "SELL"
    if momentum_return >= Decimal("0.02"):
        return "BUY"

    return "HOLD"


def calculate_twenty_day_momentum(closes: list[Decimal]) -> Decimal:
    if len(closes) < 21:
        raise ValueError("At least 21 closing prices are required for 20-day momentum.")

    return Decimal((closes[-1] / closes[-21]) - 1)


def calculate_momentum_score(momentum_return: Decimal) -> Decimal:
    score = Decimal("0.5") + (momentum_return / Decimal("0.20"))

    return max(Decimal(0), min(score, Decimal(1)))


def build_momentum_backtest_observation(
    *,
    exchange: str,
    symbol: str,
    input_dataset_version: str,
    historical_closes: list[tuple[date, Decimal]],
    future_closes: list[tuple[date, Decimal]],
) -> MomentumBacktestObservation:
    """Build one completed baseline-momentum backtest observation."""
    if len(historical_closes) != 21:
        raise ValueError("Momentum backtests require exactly 21 historical closes.")

    if len(future_closes) != 20:
        raise ValueError("Momentum backtests require exactly 20 future closes.")

    as_of_date, reference_price = historical_closes[-1]
    future_price_date, future_close_price = future_closes[-1]

    if future_price_date <= as_of_date:
        raise ValueError("Future closes must occur after the backtest as-of date.")

    momentum_return = calculate_twenty_day_momentum(
        [close_price for _, close_price in historical_closes]
    )
    score = calculate_momentum_score(momentum_return)
    action = select_momentum_action(momentum_return)

    return MomentumBacktestObservation(
        symbol=symbol,
        as_of_date=as_of_date,
        reference_price=reference_price,
        momentum_return=momentum_return,
        score=score,
        action=action,
        future_price_date=future_price_date,
        future_close_price=future_close_price,
        forward_return=calculate_forward_return(
            reference_price,
            future_close_price,
        ),
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version=input_dataset_version,
    )


def build_momentum_backtest_observations(
    *,
    exchange: str,
    symbol: str,
    input_dataset_version: str,
    closes: list[tuple[date, Decimal]],
    start_date: date,
    end_date: date,
    step_observations: int = 1,
) -> list[MomentumBacktestObservation]:
    """Build completed momentum backtest observations for an inclusive date range."""
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")

    if step_observations <= 0:
        raise ValueError("step_observations must be greater than zero.")

    if any(
        current_date >= next_date
        for (current_date, _), (next_date, _) in pairwise(closes)
    ):
        raise ValueError("closes must contain unique dates in ascending order.")

    candidate_indices = [
        index
        for index in range(20, len(closes) - 20)
        if start_date <= closes[index][0] <= end_date
    ]

    observations: list[MomentumBacktestObservation] = []

    for index in candidate_indices[::step_observations]:
        observation = build_momentum_backtest_observation(
            exchange=exchange,
            symbol=symbol,
            input_dataset_version=input_dataset_version,
            historical_closes=closes[index - 20 : index + 1],
            future_closes=closes[index + 1 : index + 21],
        )
        observations.append(observation)

    return observations
