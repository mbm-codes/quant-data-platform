from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking import MomentumRankedCandidate


def valid_ranked_candidate() -> dict[str, object]:
    return {
        "rank": 1,
        "exchange": "NSE",
        "symbol": "reliance",
        "as_of_date": date(2024, 2, 1),
        "price_date": date(2024, 2, 1),
        "reference_price": Decimal("2900.00"),
        "momentum_return": Decimal("0.08"),
        "score": Decimal("0.90"),
        "action": "buy",
        "strategy_name": "baseline_momentum",
        "strategy_version": "v1",
        "input_dataset_version": "test-v1",
    }


def test_accepts_and_normalizes_ranked_candidate() -> None:
    candidate = MomentumRankedCandidate.model_validate(valid_ranked_candidate())

    assert candidate.rank == 1
    assert candidate.symbol == "RELIANCE"
    assert candidate.action == "BUY"


def test_rejects_non_positive_rank() -> None:
    record = valid_ranked_candidate()
    record["rank"] = 0

    with pytest.raises(ValidationError, match="greater than 0"):
        MomentumRankedCandidate.model_validate(record)


def test_rejects_price_date_after_as_of_date() -> None:
    record = valid_ranked_candidate()
    record["price_date"] = date(2024, 2, 2)

    with pytest.raises(
        ValidationError,
        match="price_date must be on or before as_of_date",
    ):
        MomentumRankedCandidate.model_validate(record)
