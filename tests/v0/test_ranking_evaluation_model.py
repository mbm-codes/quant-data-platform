from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking_evaluation import (
    MomentumRankingCandidateEvaluation,
)


def valid_completed_evaluation() -> dict[str, object]:
    return {
        "ranking_run_id": "ranking-run-001",
        "exchange": "NSE",
        "symbol": "reliance",
        "rank": 1,
        "action": "buy",
        "price_date": date(2024, 1, 1),
        "reference_price": Decimal(100),
        "input_dataset_version": "test-v1",
        "evaluation_as_of_date": date(2024, 2, 1),
        "future_observations_found": 20,
        "status": "COMPLETED",
        "future_price_date": date(2024, 1, 21),
        "future_close_price": Decimal(110),
        "forward_return": Decimal("0.10"),
    }


def test_accepts_completed_ranking_candidate_evaluation() -> None:
    evaluation = MomentumRankingCandidateEvaluation.model_validate(
        valid_completed_evaluation()
    )

    assert evaluation.symbol == "RELIANCE"
    assert evaluation.action == "BUY"
    assert evaluation.status == "COMPLETED"


def test_rejects_completed_evaluation_missing_return() -> None:
    record = valid_completed_evaluation()
    record["forward_return"] = None

    with pytest.raises(
        ValidationError,
        match="require future price and return",
    ):
        MomentumRankingCandidateEvaluation.model_validate(record)


def test_rejects_pending_evaluation_with_outcome_fields() -> None:
    record = valid_completed_evaluation()
    record["status"] = "PENDING"

    with pytest.raises(
        ValidationError,
        match="Pending evaluations cannot include",
    ):
        MomentumRankingCandidateEvaluation.model_validate(record)
