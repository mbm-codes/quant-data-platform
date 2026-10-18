from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.recommendations import PortfolioRecommendation


def valid_recommendation() -> dict:
    return {
        "recommendation_id": "rec-001",
        "account_id": "main",
        "symbol": "reliance",
        "action": "buy",
        "as_of_date": date(2024, 1, 5),
        "price_date": date(2024, 1, 5),
        "reference_price": Decimal("2607.70"),
        "score": Decimal("0.82"),
        "rationale": "Positive deterministic baseline signal.",
        "strategy_name": "baseline_momentum",
        "strategy_version": "v1",
        "generated_at": datetime.now(UTC),
        "run_id": "test-run",
        "input_dataset_version": "nse-bhavcopy-v1",
    }


def test_normalizes_recommendation_symbol_and_action() -> None:
    recommendation = PortfolioRecommendation.model_validate(valid_recommendation())

    assert recommendation.symbol == "RELIANCE"
    assert recommendation.action == "BUY"
    assert recommendation.score == Decimal("0.82")


def test_rejects_price_date_after_as_of_date() -> None:
    recommendation = valid_recommendation()
    recommendation["price_date"] = date(2024, 1, 6)

    with pytest.raises(ValidationError, match="price_date"):
        PortfolioRecommendation.model_validate(recommendation)


def test_rejects_naive_generation_timestamp() -> None:
    recommendation = valid_recommendation()
    recommendation["generated_at"] = datetime.now(UTC).replace(tzinfo=None)

    with pytest.raises(ValidationError, match="timezone"):
        PortfolioRecommendation.model_validate(recommendation)
