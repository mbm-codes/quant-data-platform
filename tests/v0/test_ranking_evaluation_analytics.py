from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

import pytest

from core.models.ranking_evaluation import MomentumRankingCandidateEvaluation
from core.ranking_evaluation_analytics import summarize_ranking_evaluations


def make_evaluation(
    *,
    rank: int,
    symbol: str,
    action: Literal["BUY", "HOLD", "SELL"],
    status: Literal["PENDING", "COMPLETED"],
    forward_return: Decimal | None,
) -> MomentumRankingCandidateEvaluation:
    is_completed = status == "COMPLETED"

    return MomentumRankingCandidateEvaluation(
        ranking_run_id="ranking-run-001",
        exchange="NSE",
        symbol=symbol,
        rank=rank,
        action=action,
        price_date=date(2024, 1, 1),
        reference_price=Decimal(100),
        input_dataset_version="test-v1",
        evaluation_as_of_date=date(2024, 2, 1),
        future_observations_found=20 if is_completed else 19,
        status=status,
        future_price_date=date(2024, 1, 21) if is_completed else None,
        future_close_price=Decimal(110) if is_completed else None,
        forward_return=forward_return,
    )


def test_summarizes_completed_ranking_evaluations_by_action() -> None:
    evaluations = [
        make_evaluation(
            rank=1,
            symbol="ALPHA",
            action="BUY",
            status="COMPLETED",
            forward_return=Decimal("0.10"),
        ),
        make_evaluation(
            rank=2,
            symbol="BETA",
            action="BUY",
            status="COMPLETED",
            forward_return=Decimal("-0.02"),
        ),
        make_evaluation(
            rank=3,
            symbol="GAMMA",
            action="SELL",
            status="COMPLETED",
            forward_return=Decimal("-0.05"),
        ),
        make_evaluation(
            rank=4,
            symbol="DELTA",
            action="HOLD",
            status="COMPLETED",
            forward_return=Decimal("0.01"),
        ),
        make_evaluation(
            rank=5,
            symbol="EPSILON",
            action="BUY",
            status="PENDING",
            forward_return=None,
        ),
    ]

    summaries = summarize_ranking_evaluations(
        evaluations,
        generated_at=datetime(2024, 2, 2, tzinfo=UTC),
    )

    summaries_by_action = {summary.action: summary for summary in summaries}

    buy = summaries_by_action["BUY"]
    assert buy.completed_count == 2
    assert buy.average_forward_return == Decimal("0.04")
    assert buy.median_forward_return == Decimal("0.04")
    assert buy.directional_correct_count == 1
    assert buy.directional_hit_rate == Decimal("0.5")

    sell = summaries_by_action["SELL"]
    assert sell.completed_count == 1
    assert sell.average_forward_return == Decimal("-0.05")
    assert sell.median_forward_return == Decimal("-0.05")
    assert sell.directional_correct_count == 1
    assert sell.directional_hit_rate == Decimal(1)

    hold = summaries_by_action["HOLD"]
    assert hold.completed_count == 1
    assert hold.average_forward_return == Decimal("0.01")
    assert hold.median_forward_return == Decimal("0.01")
    assert hold.directional_correct_count is None
    assert hold.directional_hit_rate is None


def test_rejects_pending_only_ranking_evaluations() -> None:
    evaluations = [
        make_evaluation(
            rank=1,
            symbol="ALPHA",
            action="BUY",
            status="PENDING",
            forward_return=None,
        )
    ]

    with pytest.raises(
        ValueError,
        match="No completed ranking evaluations are available",
    ):
        summarize_ranking_evaluations(
            evaluations,
            generated_at=datetime(2024, 2, 2, tzinfo=UTC),
        )


def test_rejects_evaluations_from_multiple_ranking_runs() -> None:
    first = make_evaluation(
        rank=1,
        symbol="ALPHA",
        action="BUY",
        status="COMPLETED",
        forward_return=Decimal("0.10"),
    )

    second = make_evaluation(
        rank=2,
        symbol="BETA",
        action="SELL",
        status="COMPLETED",
        forward_return=Decimal("-0.05"),
    ).model_copy(update={"ranking_run_id": "different-ranking-run"})

    with pytest.raises(
        ValueError,
        match="All evaluations must belong to one ranking run",
    ):
        summarize_ranking_evaluations(
            [first, second],
            generated_at=datetime(2024, 2, 2, tzinfo=UTC),
        )
