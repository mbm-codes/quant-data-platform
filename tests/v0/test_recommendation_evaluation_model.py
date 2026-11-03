from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.recommendation_evaluation import RecommendationEvaluation


def valid_completed_evaluation() -> dict[str, object]:
    return {
        "recommendation_id": "rec-001",
        "evaluation_as_of_date": date(2024, 2, 15),
        "future_observations_found": 20,
        "status": "COMPLETED",
        "future_price_date": date(2024, 2, 1),
        "future_close_price": Decimal("110.00"),
        "forward_return": Decimal("0.10"),
    }


def test_accepts_pending_evaluation_without_outcome_fields() -> None:
    evaluation = RecommendationEvaluation.model_validate(
        {
            "recommendation_id": "rec-001",
            "evaluation_as_of_date": date(2024, 1, 15),
            "future_observations_found": 12,
            "status": "PENDING",
        }
    )

    assert evaluation.status == "PENDING"
    assert evaluation.future_price_date is None
    assert evaluation.future_close_price is None
    assert evaluation.forward_return is None


def test_accepts_completed_evaluation_with_outcome_fields() -> None:
    evaluation = RecommendationEvaluation.model_validate(valid_completed_evaluation())

    assert evaluation.status == "COMPLETED"
    assert evaluation.future_price_date == date(2024, 2, 1)
    assert evaluation.future_close_price == Decimal("110.00")
    assert evaluation.forward_return == Decimal("0.10")


def test_rejects_completed_evaluation_missing_forward_return() -> None:
    record = valid_completed_evaluation()
    record["forward_return"] = None

    with pytest.raises(ValidationError, match="require future price and return"):
        RecommendationEvaluation.model_validate(record)


def test_rejects_completed_evaluation_with_less_than_twenty_observations() -> None:
    record = valid_completed_evaluation()
    record["future_observations_found"] = 19

    with pytest.raises(ValidationError, match="at least 20 future observations"):
        RecommendationEvaluation.model_validate(record)


def test_rejects_pending_evaluation_that_includes_forward_return() -> None:
    record = valid_completed_evaluation()
    record["status"] = "PENDING"

    with pytest.raises(ValidationError, match="Pending evaluations cannot include"):
        RecommendationEvaluation.model_validate(record)
