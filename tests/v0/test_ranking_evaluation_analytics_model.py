from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking_evaluation_analytics import MomentumRankingActionSummary


def valid_buy_summary() -> dict[str, object]:
    return {
        "ranking_run_id": "test-run-1",
        "input_dataset_version": "test-v1",
        "action": "buy",
        "completed_count": 2,
        "average_forward_return": Decimal("0.04"),
        "median_forward_return": Decimal("0.04"),
        "directional_correct_count": 1,
        "directional_hit_rate": Decimal("0.5"),
        "evaluation_as_of_date": date(2024, 1, 1),
        "generated_at": datetime(2024, 1, 3, tzinfo=UTC),
    }


def test_accepts_valid_buy_summary() -> None:
    evaluation = MomentumRankingActionSummary.model_validate(valid_buy_summary())

    assert evaluation.ranking_run_id == "test-run-1"
    assert evaluation.action == "BUY"
    assert evaluation.completed_count == 2
    assert evaluation.evaluation_as_of_date == date(2024, 1, 1)
    assert evaluation.directional_hit_rate == Decimal("0.5")


def test_accepts_hold_summary_with_none_directional_fields() -> None:
    record = valid_buy_summary()
    record["action"] = "hold"
    record["directional_correct_count"] = None
    record["directional_hit_rate"] = None

    action_summary = MomentumRankingActionSummary.model_validate(record)
    assert action_summary.ranking_run_id == "test-run-1"
    assert action_summary.action == "HOLD"
    assert action_summary.completed_count == 2
    assert action_summary.evaluation_as_of_date == date(2024, 1, 1)
    assert action_summary.directional_hit_rate is None
    assert action_summary.directional_correct_count is None


def test_rejects_directional_hit_rate_above_one() -> None:
    record = valid_buy_summary()
    record["directional_hit_rate"] = Decimal("1.5")

    with pytest.raises(ValidationError):
        MomentumRankingActionSummary.model_validate(record)


def test_rejects_hold_with_populated_directional_fields() -> None:
    record = valid_buy_summary()
    record["directional_hit_rate"] = Decimal("0.5")
    record["directional_correct_count"] = 1
    record["action"] = "hold"

    with pytest.raises(ValidationError):
        MomentumRankingActionSummary.model_validate(record)


def test_rejects_generated_at_before_evaluation_as_of_date() -> None:
    record = valid_buy_summary()
    record["generated_at"] = datetime(2023, 12, 25, 12, tzinfo=UTC)

    with pytest.raises(
        ValidationError, match="evaluation_as_of_date cannot be after generated_at."
    ):
        MomentumRankingActionSummary.model_validate(record)


def test_rejects_naive_generated_at() -> None:
    record = valid_buy_summary()
    record["generated_at"] = datetime(2023, 12, 25, 12)  # noqa: DTZ001

    with pytest.raises(
        ValidationError, match="generated_at must include timezone information."
    ):
        MomentumRankingActionSummary.model_validate(record)
