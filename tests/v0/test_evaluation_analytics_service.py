from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from core.data.duckdb import (
    connect,
    register_recommendation_evaluations_view,
    register_recommendations_view,
)
from core.data.recommendation_evaluation_ledger import (
    write_completed_recommendation_evaluations,
)
from core.data.recommendation_ledger import write_recommendation_ledger
from core.models.recommendation_evaluation import RecommendationEvaluation
from core.models.recommendations import PortfolioRecommendation
from core.services.evaluation_analytics import (
    summarize_recorded_evaluations,
)


def test_returns_no_summaries_when_evaluation_ledger_is_empty(
    tmp_path: Path,
) -> None:
    summaries = summarize_recorded_evaluations(
        database_path=tmp_path / "qdp.duckdb",
        recommendations_directory=tmp_path / "recommendations",
        evaluations_directory=tmp_path / "recommended_evaluations",
    )

    assert summaries == []


def test_summarizes_completed_evaluations(
    tmp_path: Path,
) -> None:
    recommendations_directory = tmp_path / "recommendations"
    evaluations_directory = tmp_path / "recommendation_evaluations"

    recommendations = [
        PortfolioRecommendation(
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
        ),
        PortfolioRecommendation(
            recommendation_id="rec-002",
            account_id="main",
            exchange="NSE",
            symbol="TCS",
            action="BUY",
            as_of_date=date(2024, 1, 2),
            price_date=date(2024, 1, 2),
            reference_price=Decimal(200),
            score=Decimal("0.75"),
            rationale="Test recommendation.",
            strategy_name="baseline_momentum",
            strategy_version="v1",
            generated_at=datetime(2024, 1, 2, tzinfo=UTC),
            run_id="test-run",
            input_dataset_version="test-v1",
        ),
    ]
    evaluations = [
        RecommendationEvaluation(
            recommendation_id="rec-001",
            evaluation_as_of_date=date(2024, 2, 1),
            future_observations_found=20,
            status="COMPLETED",
            future_price_date=date(2024, 1, 21),
            future_close_price=Decimal(110),
            forward_return=Decimal("0.10"),
        ),
        RecommendationEvaluation(
            recommendation_id="rec-002",
            evaluation_as_of_date=date(2024, 2, 2),
            future_observations_found=20,
            status="COMPLETED",
            future_price_date=date(2024, 1, 22),
            future_close_price=Decimal(196),
            forward_return=Decimal("-0.02"),
        ),
    ]

    recommendation_paths = write_recommendation_ledger(
        recommendations,
        recommendations_directory,
    )
    evaluation_paths = write_completed_recommendation_evaluations(
        evaluations,
        evaluations_directory,
    )

    assert len(recommendation_paths) == 2
    assert all(path.exists() for path in recommendation_paths)
    assert len(evaluation_paths) == 2
    assert all(path.exists() for path in evaluation_paths)

    connection = connect(tmp_path / "debug.duckdb")
    try:
        register_recommendations_view(
            connection,
            recommendations_directory,
        )
        register_recommendation_evaluations_view(
            connection,
            evaluations_directory,
        )

        recommendation_rows = connection.execute(
            """
            SELECT recommendation_id
            FROM recommendations
            ORDER BY recommendation_id
            """
        ).fetchall()
        evaluation_rows = connection.execute(
            """
            SELECT recommendation_id, forward_return
            FROM recommendation_evaluations
            ORDER BY recommendation_id
            """
        ).fetchall()
    finally:
        connection.close()

    assert recommendation_rows == [("rec-001",), ("rec-002",)]
    assert evaluation_rows == [
        ("rec-001", Decimal("0.10")),
        ("rec-002", Decimal("-0.02")),
    ]

    summaries = summarize_recorded_evaluations(
        database_path=tmp_path / "qdp.duckdb",
        recommendations_directory=recommendations_directory,
        evaluations_directory=evaluations_directory,
    )

    assert len(summaries) == 1
    assert summaries[0].action == "BUY"
    assert summaries[0].completed_count == 2
    assert summaries[0].average_forward_return == Decimal("0.04")
    assert summaries[0].directional_correct_count == 1
    assert summaries[0].directional_hit_rate == Decimal("0.5")
