from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from core.data.duckdb import connect, register_daily_prices_view
from core.data.price_history import load_historical_closes_by_symbol
from core.models.ranking_backtest import (
    MomentumRankingBacktestObservation,
    MomentumRankingBacktestSummary,
)
from core.ranking_backtest import (
    build_momentum_ranking_backtest_observations,
    summarize_momentum_ranking_backtest,
)
from core.services.universe import calculate_universe_hash

FORWARD_HORIZON_OBSERVATIONS = 20


@dataclass(frozen=True)
class MomentumRankingBacktestResult:
    observations: list[MomentumRankingBacktestObservation]
    summary: MomentumRankingBacktestSummary | None
    attempted_ranking_date_count: int


def run_momentum_ranking_backtest(
    *,
    exchange: Literal["NSE"],
    start_date: date,
    end_date: date,
    evaluation_as_of_date: date,
    allowed_symbols: set[str],
    bucket_size: int,
    step_observations: int,
    input_dataset_version: str,
    database_path: Path,
    daily_prices_directory: Path,
    generated_at: datetime,
) -> MomentumRankingBacktestResult:
    """Run a reproducible rolling momentum-ranking backtest."""
    if not allowed_symbols:
        raise ValueError("allowed_symbols cannot be empty.")

    connection = connect(database_path)

    try:
        register_daily_prices_view(
            connection,
            daily_prices_directory,
        )
        closes_by_symbol = load_historical_closes_by_symbol(
            connection,
            exchange=exchange,
            dataset_version=input_dataset_version,
            evaluation_as_of_date=evaluation_as_of_date,
            allowed_symbols=allowed_symbols,
        )
    finally:
        connection.close()

    build_result = build_momentum_ranking_backtest_observations(
        closes_by_symbol,
        start_date=start_date,
        end_date=end_date,
        evaluation_as_of_date=evaluation_as_of_date,
        bucket_size=bucket_size,
        step_observations=step_observations,
        input_dataset_version=input_dataset_version,
        universe_hash=calculate_universe_hash(allowed_symbols),
        generated_at=generated_at,
    )

    summary: MomentumRankingBacktestSummary | None = None

    if build_result.observations:
        summary = summarize_momentum_ranking_backtest(
            build_result.observations,
            attempted_ranking_date_count=(build_result.attempted_ranking_date_count),
            start_date=start_date,
            end_date=end_date,
            step_observations=step_observations,
            forward_horizon_observations=FORWARD_HORIZON_OBSERVATIONS,
            generated_at=generated_at,
        )

    return MomentumRankingBacktestResult(
        observations=build_result.observations,
        summary=summary,
        attempted_ranking_date_count=(build_result.attempted_ranking_date_count),
    )
