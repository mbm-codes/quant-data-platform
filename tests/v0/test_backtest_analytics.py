from datetime import date
from decimal import Decimal

from core.backtest_analytics import summarize_momentum_backtest
from core.models.backtest import (
    BacktestAction,
    MomentumBacktestObservation,
)


def make_observation(
    action: BacktestAction,
    forward_return: str,
) -> MomentumBacktestObservation:
    return MomentumBacktestObservation(
        exchange="NSE",
        symbol="RELIANCE",
        as_of_date=date(2024, 1, 1),
        reference_price=Decimal(100),
        momentum_return=Decimal("0.05"),
        score=Decimal("0.75"),
        action=action,
        future_price_date=date(2024, 1, 21),
        future_close_price=Decimal(110),
        forward_return=Decimal(forward_return),
        strategy_name="baseline_momentum",
        strategy_version="v1",
        input_dataset_version="test-v1",
    )


def test_reports_median_forward_return() -> None:
    summaries = summarize_momentum_backtest(
        [
            make_observation("BUY", "0.20"),
            make_observation("BUY", "-0.02"),
            make_observation("BUY", "0.01"),
        ]
    )

    assert len(summaries) == 1
    assert summaries[0].action == "BUY"
    assert summaries[0].median_forward_return == Decimal("0.01")
