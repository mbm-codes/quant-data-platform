from datetime import date
from pathlib import Path

from core.data.duckdb import connect, register_recommendations_view
from core.data.recommendation_evaluation_ledger import (
    write_completed_recommendation_evaluations,
)
from core.data.recommendation_reader import load_recommendation
from core.models.recommendation_evaluation import RecommendationEvaluation
from core.models.recommendations import PortfolioRecommendation
from core.services.recommendation_evaluation import evaluate_recommendation


def evaluate_and_record_recommendation(
    recommendation: PortfolioRecommendation,
    *,
    evaluation_as_of_date: date,
    database_path: Path,
    daily_prices_directory: Path,
    evaluation_ledger_directory: Path,
) -> tuple[RecommendationEvaluation, Path | None]:
    """Evaluate one recommendation and persist it only when completed."""
    evaluation = evaluate_recommendation(
        recommendation,
        evaluation_as_of_date=evaluation_as_of_date,
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
    )

    if evaluation.status == "PENDING":
        return evaluation, None

    written_paths = write_completed_recommendation_evaluations(
        [evaluation],
        evaluation_ledger_directory,
    )

    return evaluation, written_paths[0]


def evaluate_and_record_recommendation_by_id(
    recommendation_id: str,
    *,
    evaluation_as_of_date: date,
    database_path: Path,
    recommendations_directory: Path,
    daily_prices_directory: Path,
    evaluation_ledger_directory: Path,
) -> tuple[RecommendationEvaluation, Path | None]:
    """Load, evaluate, and conditionally record one recommendation by ID."""
    connection = connect(database_path)
    try:
        register_recommendations_view(connection, recommendations_directory)
        recommendation = load_recommendation(
            connection,
            recommendation_id=recommendation_id,
        )
    finally:
        connection.close()

    return evaluate_and_record_recommendation(
        recommendation,
        evaluation_as_of_date=evaluation_as_of_date,
        database_path=database_path,
        daily_prices_directory=daily_prices_directory,
        evaluation_ledger_directory=evaluation_ledger_directory,
    )
