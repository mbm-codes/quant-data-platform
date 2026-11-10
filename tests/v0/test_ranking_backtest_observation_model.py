from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking_backtest import (
    MomentumRankingBacktestObservation,
)


def valid_backtest_observation() -> dict[str, object]:
    return {
        "strategy_name": "baseline_momentum",
        "strategy_version": "v1",
        "input_dataset_version": "test-v1",
        "universe_hash": "a" * 64,
        "ranking_date": date(2024, 1, 1),
        "evaluation_as_of_date": date(2024, 2, 1),
        "bucket_size": 2,
        "top_rank_start": 1,
        "top_rank_end": 2,
        "bottom_rank_start": 5,
        "bottom_rank_end": 6,
        "top_completed_count": 2,
        "bottom_completed_count": 2,
        "top_average_forward_return": Decimal("0.06"),
        "bottom_average_forward_return": Decimal("-0.03"),
        "rank_spread": Decimal("0.09"),
        "top_positive_return_rate": Decimal(1),
        "bottom_positive_return_rate": Decimal(0),
        "generated_at": datetime(2024, 2, 2, tzinfo=UTC),
    }


def test_accepts_valid_ranking_backtest_observation() -> None:
    observation = MomentumRankingBacktestObservation.model_validate(
        valid_backtest_observation()
    )

    assert observation.exchange == "NSE"
    assert observation.top_rank_start == 1
    assert observation.bottom_rank_end == 6
    assert observation.rank_spread == Decimal("0.09")


def test_rejects_overlapping_rank_ranges() -> None:
    record = valid_backtest_observation()
    record["bottom_rank_start"] = 2

    with pytest.raises(
        ValidationError,
        match="Top and bottom rank ranges cannot overlap",
    ):
        MomentumRankingBacktestObservation.model_validate(record)


def test_rejects_incorrect_rank_spread() -> None:
    record = valid_backtest_observation()
    record["rank_spread"] = Decimal("0.08")

    with pytest.raises(
        ValidationError,
        match="rank_spread must equal top average return minus",
    ):
        MomentumRankingBacktestObservation.model_validate(record)


def test_rejects_naive_generated_at() -> None:
    record = valid_backtest_observation()
    record["generated_at"] = datetime(  # noqa: DTZ001
        2024,
        2,
        2,
        12,
    )

    with pytest.raises(
        ValidationError,
        match="generated_at must include timezone information",
    ):
        MomentumRankingBacktestObservation.model_validate(record)
