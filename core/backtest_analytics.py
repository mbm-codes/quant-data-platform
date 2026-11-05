from collections import defaultdict
from decimal import Decimal
from statistics import median

from core.models.backtest import (
    BacktestAction,
    MomentumBacktestActionSummary,
    MomentumBacktestObservation,
)


def summarize_momentum_backtest(
    observations: list[MomentumBacktestObservation],
) -> list[MomentumBacktestActionSummary]:
    """Summarize momentum outcomes by recommended action."""
    grouped_observations: dict[
        BacktestAction,
        list[MomentumBacktestObservation],
    ] = defaultdict(list)

    for observation in observations:
        grouped_observations[observation.action].append(observation)

    summaries: list[MomentumBacktestActionSummary] = []
    actions: tuple[BacktestAction, ...] = ("BUY", "HOLD", "SELL")

    for action in actions:
        action_observations = grouped_observations[action]

        if not action_observations:
            continue

        observation_count = len(action_observations)
        average_forward_return = sum(
            (observation.forward_return for observation in action_observations),
            start=Decimal(0),
        ) / Decimal(observation_count)

        median_forward_return = median(
            observation.forward_return for observation in action_observations
        )

        directional_observations, directional_correct_count = _directional_results(
            action, action_observations
        )
        directional_hit_rate = (
            Decimal(directional_correct_count) / Decimal(directional_observations)
            if directional_observations > 0
            else None
        )

        summaries.append(
            MomentumBacktestActionSummary(
                action=action,
                observation_count=observation_count,
                average_forward_return=average_forward_return,
                directional_observations=directional_observations,
                directional_correct_count=directional_correct_count,
                directional_hit_rate=directional_hit_rate,
                median_forward_return=median_forward_return,
            )
        )

    return summaries


def _directional_results(
    action: str,
    observations: list[MomentumBacktestObservation],
) -> tuple[int, int]:
    if action == "BUY":
        return (
            len(observations),
            sum(observation.forward_return > 0 for observation in observations),
        )

    if action == "SELL":
        return (
            len(observations),
            sum(observation.forward_return < 0 for observation in observations),
        )

    return 0, 0
