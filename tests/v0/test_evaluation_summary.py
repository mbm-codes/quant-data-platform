from datetime import date
from decimal import Decimal

from core.evaluation_analytics import summarize_completed_evaluations
from core.models.evaluation_analytics import CompletedEvaluationRow


def make_row(
    recommendation_id: str,
    action: str,
    forward_return: str,
) -> CompletedEvaluationRow:
    return CompletedEvaluationRow(
        recommendation_id=recommendation_id,
        account_id="main",
        strategy_name="baseline_momentum",
        strategy_version="v1",
        action=action,
        symbol="RELIANCE",
        as_of_date=date(2024, 1, 1),
        future_price_date=date(2024, 1, 1),
        forward_return=Decimal(forward_return),
    )


def test_summarizes_returns_and_directional_accuracy() -> None:
    summaries = summarize_completed_evaluations(
        [
            make_row("buy-correct", "BUY", "0.10"),
            make_row("buy-wrong", "BUY", "-0.02"),
            make_row("sell-correct", "SELL", "-0.10"),
            make_row("sell-wrong", "SELL", "0.05"),
            make_row("hold", "HOLD", "0.00"),
        ]
    )

    by_action = {summary.action: summary for summary in summaries}

    buy = by_action["BUY"]
    assert buy.completed_count == 2
    assert buy.average_forward_return == Decimal("0.04")
    assert buy.directional_observations == 2
    assert buy.directional_correct_count == 1
    assert buy.directional_hit_rate == Decimal("0.5")

    sell = by_action["SELL"]
    assert sell.completed_count == 2
    assert sell.average_forward_return == Decimal("-0.025")
    assert sell.directional_observations == 2
    assert sell.directional_correct_count == 1
    assert sell.directional_hit_rate == Decimal("0.5")

    hold = by_action["HOLD"]
    assert hold.completed_count == 1
    assert hold.average_forward_return == Decimal(0)
    assert hold.directional_observations == 0
    assert hold.directional_correct_count == 0
    assert hold.directional_hit_rate is None
