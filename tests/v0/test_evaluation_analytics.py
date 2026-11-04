# tests/v0/test_evaluation_analytics.py

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.data.duckdb import (
    connect,
    register_recommendation_evaluations_view,
    register_recommendations_view,
)
from core.data.evaluation_analytics import load_completed_evaluation_rows
from core.data.recommendation_evaluation_ledger import (
    write_completed_recommendation_evaluations,
)
from core.data.recommendation_ledger import write_recommendation_ledger
from core.models.recommendation_evaluation import RecommendationEvaluation
from core.models.recommendations import PortfolioRecommendation


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


def make_evaluation() -> RecommendationEvaluation:
    return RecommendationEvaluation(
        recommendation_id="rec-001",
        evaluation_as_of_date=date(2024, 2, 1),
        future_observations_found=20,
        status="COMPLETED",
        future_price_date=date(2024, 1, 21),
        future_close_price=Decimal(110),
        forward_return=Decimal("0.10"),
    )


def test_loads_completed_evaluations_with_recommendation_context(
    tmp_path: Path,
) -> None:
    recommendations_directory = tmp_path / "recommendations"
    evaluations_directory = tmp_path / "recommendation_evaluations"

    recommendation_paths = write_recommendation_ledger(
        [make_recommendation()],
        recommendations_directory,
    )
    assert len(recommendation_paths) == 1
    assert recommendation_paths[0].exists()
    assert (
        recommendation_paths[0].parent == recommendations_directory / "as_of_year=2024"
    )

    evaluation_paths = write_completed_recommendation_evaluations(
        [make_evaluation()],
        evaluations_directory,
    )
    assert len(evaluation_paths) == 1
    assert evaluation_paths[0].exists()
    assert evaluation_paths[0].parent == evaluations_directory / "evaluation_year=2024"

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_recommendations_view(
            connection,
            recommendations_directory,
        )
        register_recommendation_evaluations_view(
            connection,
            evaluations_directory,
        )

        rows = load_completed_evaluation_rows(connection)
    finally:
        connection.close()

    assert len(rows) == 1
    assert rows[0].recommendation_id == "rec-001"
    assert rows[0].strategy_name == "baseline_momentum"
    assert rows[0].action == "BUY"
    assert rows[0].symbol == "RELIANCE"
    assert rows[0].forward_return == Decimal("0.10")
