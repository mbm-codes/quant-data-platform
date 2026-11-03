from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.duckdb import connect, register_recommendations_view
from core.data.recommendation_ledger import write_recommendation_ledger
from core.data.recommendation_reader import load_recommendation
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


def test_loads_recorded_recommendation(tmp_path: Path) -> None:
    ledger_directory = tmp_path / "recommendations"
    write_recommendation_ledger([make_recommendation()], ledger_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_recommendations_view(connection, ledger_directory)

        recommendation = load_recommendation(
            connection,
            recommendation_id="rec-001",
        )
    finally:
        connection.close()

    assert recommendation.recommendation_id == "rec-001"
    assert recommendation.symbol == "RELIANCE"
    assert recommendation.input_dataset_version == "test-v1"


def test_rejects_missing_recommendation(tmp_path: Path) -> None:
    ledger_directory = tmp_path / "recommendations"
    write_recommendation_ledger([make_recommendation()], ledger_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_recommendations_view(connection, ledger_directory)

        with pytest.raises(ValueError, match="Recommendation not found"):
            load_recommendation(
                connection,
                recommendation_id="missing-rec",
            )
    finally:
        connection.close()
