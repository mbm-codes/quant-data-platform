from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from statistics import median

from core.models.ranking import MomentumRankedCandidate
from core.models.ranking_backtest import (
    MomentumRankingBacktestObservation,
    MomentumRankingBacktestSummary,
)
from core.recommendation_evaluation import calculate_forward_return
from core.strategies.momentum import rank_momentum_candidates


class IncompleteRankingBacktestDataError(ValueError):
    """A ranking date cannot be fully evaluated with available history."""


@dataclass(frozen=True)
class MomentumRankingBacktestBuildResult:
    observations: list[MomentumRankingBacktestObservation]
    attempted_ranking_date_count: int


def build_momentum_ranking_backtest_observation(
    closes_by_symbol: dict[str, list[tuple[date, Decimal]]],
    *,
    ranking_date: date,
    evaluation_as_of_date: date,
    bucket_size: int,
    input_dataset_version: str,
    universe_hash: str,
    generated_at: datetime,
) -> MomentumRankingBacktestObservation:
    """Backtest one top-versus-bottom momentum ranking date."""
    if bucket_size <= 0:
        raise ValueError("bucket_size must be greater than zero.")

    if evaluation_as_of_date <= ranking_date:
        raise ValueError("evaluation_as_of_date must be after ranking_date.")

    normalized_closes_by_symbol = {
        symbol.strip().upper(): sorted(closes, key=lambda close: close[0])
        for symbol, closes in closes_by_symbol.items()
    }

    rankable_closes_by_symbol: dict[str, list[tuple[date, Decimal]]] = {}

    for symbol, closes in normalized_closes_by_symbol.items():
        historical_closes = [close for close in closes if close[0] <= ranking_date]

        if len(historical_closes) >= 21 and historical_closes[-1][0] == ranking_date:
            rankable_closes_by_symbol[symbol] = historical_closes[-21:]

    ranked_candidates = rank_momentum_candidates(
        exchange="NSE",
        as_of_date=ranking_date,
        input_dataset_version=input_dataset_version,
        closes_by_symbol=rankable_closes_by_symbol,
    )

    if len(ranked_candidates) < bucket_size * 2:
        raise IncompleteRankingBacktestDataError(
            "At least twice the bucket_size eligible candidates are required."
        )

    top_candidates = ranked_candidates[:bucket_size]
    bottom_candidates = ranked_candidates[-bucket_size:]

    top_returns = _calculate_forward_returns(
        top_candidates,
        normalized_closes_by_symbol,
        ranking_date=ranking_date,
        evaluation_as_of_date=evaluation_as_of_date,
    )
    bottom_returns = _calculate_forward_returns(
        bottom_candidates,
        normalized_closes_by_symbol,
        ranking_date=ranking_date,
        evaluation_as_of_date=evaluation_as_of_date,
    )

    top_average_return = _average(top_returns)
    bottom_average_return = _average(bottom_returns)

    return MomentumRankingBacktestObservation(
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version=input_dataset_version,
        universe_hash=universe_hash,
        ranking_date=ranking_date,
        evaluation_as_of_date=evaluation_as_of_date,
        bucket_size=bucket_size,
        top_rank_start=top_candidates[0].rank,
        top_rank_end=top_candidates[-1].rank,
        bottom_rank_start=bottom_candidates[0].rank,
        bottom_rank_end=bottom_candidates[-1].rank,
        top_completed_count=len(top_returns),
        bottom_completed_count=len(bottom_returns),
        top_average_forward_return=top_average_return,
        bottom_average_forward_return=bottom_average_return,
        rank_spread=top_average_return - bottom_average_return,
        top_positive_return_rate=_positive_return_rate(top_returns),
        bottom_positive_return_rate=_positive_return_rate(bottom_returns),
        generated_at=generated_at,
    )


def build_momentum_ranking_backtest_observations(
    closes_by_symbol: dict[str, list[tuple[date, Decimal]]],
    *,
    start_date: date,
    end_date: date,
    evaluation_as_of_date: date,
    bucket_size: int,
    step_observations: int,
    input_dataset_version: str,
    universe_hash: str,
    generated_at: datetime,
) -> MomentumRankingBacktestBuildResult:
    """Build completed bucket observations across historical ranking dates."""
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date.")

    if step_observations <= 0:
        raise ValueError("step_observations must be greater than zero.")

    available_ranking_dates = sorted(
        {
            trade_date
            for closes in closes_by_symbol.values()
            for trade_date, _ in closes
            if start_date <= trade_date <= end_date
        }
    )
    selected_ranking_dates = available_ranking_dates[::step_observations]

    if not selected_ranking_dates:
        raise ValueError("No ranking dates are available in the requested range.")

    observations: list[MomentumRankingBacktestObservation] = []

    for ranking_date in selected_ranking_dates:
        try:
            observation = build_momentum_ranking_backtest_observation(
                closes_by_symbol,
                ranking_date=ranking_date,
                evaluation_as_of_date=evaluation_as_of_date,
                bucket_size=bucket_size,
                input_dataset_version=input_dataset_version,
                universe_hash=universe_hash,
                generated_at=generated_at,
            )
        except IncompleteRankingBacktestDataError:
            continue

        observations.append(observation)

    return MomentumRankingBacktestBuildResult(
        observations=observations,
        attempted_ranking_date_count=len(selected_ranking_dates),
    )


def _calculate_forward_returns(
    candidates: list[MomentumRankedCandidate],
    closes_by_symbol: dict[str, list[tuple[date, Decimal]]],
    *,
    ranking_date: date,
    evaluation_as_of_date: date,
) -> list[Decimal]:
    forward_returns: list[Decimal] = []

    for candidate in candidates:
        future_closes = [
            close
            for close in closes_by_symbol[candidate.symbol]
            if ranking_date < close[0] <= evaluation_as_of_date
        ]

        if len(future_closes) < 20:
            raise IncompleteRankingBacktestDataError(
                f"{candidate.symbol} does not have 20 future observations."
            )

        _, future_close_price = future_closes[19]
        forward_returns.append(
            calculate_forward_return(
                candidate.reference_price,
                future_close_price,
            )
        )

    return forward_returns


def _average(values: list[Decimal]) -> Decimal:
    return sum(values, start=Decimal(0)) / Decimal(len(values))


def _positive_return_rate(values: list[Decimal]) -> Decimal:
    positive_count = sum(value > Decimal(0) for value in values)
    return Decimal(positive_count) / Decimal(len(values))


def summarize_momentum_ranking_backtest(
    observations: list[MomentumRankingBacktestObservation],
    *,
    attempted_ranking_date_count: int,
    start_date: date,
    end_date: date,
    step_observations: int,
    forward_horizon_observations: int,
    generated_at: datetime,
) -> MomentumRankingBacktestSummary:
    """Summarize rank spreads across completed historical ranking dates."""
    if not observations:
        raise ValueError("No completed ranking backtest observations exist.")

    _validate_shared_observation_metadata(observations)

    first_observation = observations[0]
    rank_spreads = [observation.rank_spread for observation in observations]
    positive_rank_spread_count = sum(
        rank_spread > Decimal(0) for rank_spread in rank_spreads
    )

    return MomentumRankingBacktestSummary(
        strategy_name=first_observation.strategy_name,
        strategy_version=first_observation.strategy_version,
        input_dataset_version=first_observation.input_dataset_version,
        universe_hash=first_observation.universe_hash,
        start_date=start_date,
        end_date=end_date,
        bucket_size=first_observation.bucket_size,
        step_observations=step_observations,
        forward_horizon_observations=forward_horizon_observations,
        attempted_ranking_date_count=attempted_ranking_date_count,
        completed_ranking_date_count=len(observations),
        positive_rank_spread_count=positive_rank_spread_count,
        rank_spread_hit_rate=(
            Decimal(positive_rank_spread_count) / Decimal(len(observations))
        ),
        average_rank_spread=(
            sum(rank_spreads, start=Decimal(0)) / Decimal(len(rank_spreads))
        ),
        median_rank_spread=median(rank_spreads),
        generated_at=generated_at,
    )


def _validate_shared_observation_metadata(
    observations: list[MomentumRankingBacktestObservation],
) -> None:
    first_observation = observations[0]

    for observation in observations[1:]:
        if observation.strategy_name != first_observation.strategy_name:
            raise ValueError("All observations must use one strategy name.")

        if observation.strategy_version != first_observation.strategy_version:
            raise ValueError("All observations must use one strategy version.")

        if observation.input_dataset_version != first_observation.input_dataset_version:
            raise ValueError("All observations must use one input dataset version.")

        if observation.universe_hash != first_observation.universe_hash:
            raise ValueError("All observations must use one universe hash.")

        if observation.bucket_size != first_observation.bucket_size:
            raise ValueError("All observations must use one bucket size.")
