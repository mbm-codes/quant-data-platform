from datetime import date
from pathlib import Path

from core.data.duckdb import (
    connect,
    register_daily_prices_view,
    register_momentum_ranking_snapshot_views,
)
from core.data.price_history import load_future_closes
from core.data.ranking_snapshot_reader import load_momentum_ranking_snapshot
from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.recommendation_evaluation import calculate_forward_return


def evaluate_momentum_ranking_snapshot(
    *,
    ranking_run_id: str,
    evaluation_as_of_date: date,
    database_path: Path,
    ranking_snapshots_directory: Path,
    daily_prices_directory: Path,
) -> list[MomentumRankingCandidateEvaluation]:
    """Evaluate every candidate in a stored momentum-ranking snapshot."""
    connection = connect(database_path)

    try:
        register_momentum_ranking_snapshot_views(
            connection,
            ranking_snapshots_directory,
        )
        register_daily_prices_view(connection, daily_prices_directory)

        snapshot = load_momentum_ranking_snapshot(
            connection,
            ranking_run_id=ranking_run_id,
        )

        if evaluation_as_of_date < snapshot.ranking_run.as_of_date:
            raise ValueError(
                "evaluation_as_of_date cannot be before the ranking run date."
            )

        evaluations: list[MomentumRankingCandidateEvaluation] = []

        for candidate in snapshot.candidates:
            future_closes = load_future_closes(
                connection,
                exchange=candidate.exchange,
                symbol=candidate.symbol,
                dataset_version=candidate.input_dataset_version,
                price_date=candidate.price_date,
                evaluation_as_of_date=evaluation_as_of_date,
            )

            if len(future_closes) < 20:
                evaluations.append(
                    MomentumRankingCandidateEvaluation(
                        ranking_run_id=ranking_run_id,
                        exchange=candidate.exchange,
                        symbol=candidate.symbol,
                        rank=candidate.rank,
                        action=candidate.action,
                        price_date=candidate.price_date,
                        reference_price=candidate.reference_price,
                        input_dataset_version=candidate.input_dataset_version,
                        evaluation_as_of_date=evaluation_as_of_date,
                        future_observations_found=len(future_closes),
                        status="PENDING",
                    )
                )
                continue

            future_price_date, future_close_price = future_closes[-1]

            evaluations.append(
                MomentumRankingCandidateEvaluation(
                    ranking_run_id=ranking_run_id,
                    exchange=candidate.exchange,
                    symbol=candidate.symbol,
                    rank=candidate.rank,
                    action=candidate.action,
                    price_date=candidate.price_date,
                    reference_price=candidate.reference_price,
                    input_dataset_version=candidate.input_dataset_version,
                    evaluation_as_of_date=evaluation_as_of_date,
                    future_observations_found=len(future_closes),
                    status="COMPLETED",
                    future_price_date=future_price_date,
                    future_close_price=future_close_price,
                    forward_return=calculate_forward_return(
                        candidate.reference_price,
                        future_close_price,
                    ),
                )
            )

        return evaluations
    finally:
        connection.close()
