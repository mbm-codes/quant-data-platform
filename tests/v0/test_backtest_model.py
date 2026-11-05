# tests/v0/test_backtest_model.py

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.backtest import MomentumBacktestObservation


def valid_momentum_backtest_observation() -> dict[str, object]:
    return {
        "exchange": "NSE",
        "symbol": "reliance",
        "as_of_date": date(2024, 2, 1),
        "reference_price": Decimal("110.00"),
        "momentum_return": Decimal("0.10"),
        "score": Decimal("0.10"),
        "action": "buy",
        "future_price_date": date(2024, 2, 7),
        "future_close_price": Decimal("120.00"),
        "forward_return": Decimal("0.10"),
        "strategy_name": "baseline_momentum",
        "strategy_version": "v1",
        "input_dataset_version": "test-v1",
    }


def test_accepts_valid_momentum_backtest_observation() -> None:
    observation = MomentumBacktestObservation.model_validate(
        valid_momentum_backtest_observation()
    )

    assert observation.exchange == "NSE"
    assert observation.symbol == "RELIANCE"
    assert observation.future_close_price == Decimal("120.00")
    assert observation.action == "BUY"


def test_rejects_future_price_date_equal_to_as_of_date() -> None:
    record = valid_momentum_backtest_observation()
    record["future_price_date"] = date(2024, 2, 1)

    with pytest.raises(
        ValidationError,
        match="future_price_date must be after as_of_date",
    ):
        MomentumBacktestObservation.model_validate(record)


def test_rejects_score_outside_of_zero_and_one() -> None:
    record = valid_momentum_backtest_observation()
    record["score"] = Decimal("1.90")

    with pytest.raises(ValidationError, match="less than or equal to 1"):
        MomentumBacktestObservation.model_validate(record)
