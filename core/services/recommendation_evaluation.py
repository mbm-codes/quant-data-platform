from datetime import date
from pathlib import Path

from core.data.duckdb import connect, register_daily_prices_view
from core.data.price_history import load_future_closes
from core.models.recommendation_evaluation import RecommendationEvaluation
from core.models.recommendations import PortfolioRecommendation
from core.recommendation_evaluation import calculate_forward_return


def evaluate_recommendation(
    recommendation: PortfolioRecommendation,
    *,
    evaluation_as_of_date: date,
    database_path: Path,
    daily_prices_directory: Path,
) -> RecommendationEvaluation:
    """Evaluate a recommendation after up to 20 future price observations."""
    if evaluation_as_of_date < recommendation.price_date:
        raise ValueError(
            "evaluation_as_of_date cannot be before the recommendation price date."
        )

    connection = connect(database_path)
    try:
        register_daily_prices_view(connection, daily_prices_directory)

        future_closes = load_future_closes(
            connection,
            exchange=recommendation.exchange,
            symbol=recommendation.symbol,
            dataset_version=recommendation.input_dataset_version,
            price_date=recommendation.price_date,
            evaluation_as_of_date=evaluation_as_of_date,
        )
    finally:
        connection.close()

    if len(future_closes) < 20:
        return RecommendationEvaluation(
            recommendation_id=recommendation.recommendation_id,
            evaluation_as_of_date=evaluation_as_of_date,
            future_observations_found=len(future_closes),
            status="PENDING",
        )

    future_price_date, future_close_price = future_closes[-1]

    return RecommendationEvaluation(
        recommendation_id=recommendation.recommendation_id,
        evaluation_as_of_date=evaluation_as_of_date,
        future_observations_found=len(future_closes),
        status="COMPLETED",
        future_price_date=future_price_date,
        future_close_price=future_close_price,
        forward_return=calculate_forward_return(
            recommendation.reference_price,
            future_close_price,
        ),
    )
