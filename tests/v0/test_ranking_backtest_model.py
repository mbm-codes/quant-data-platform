from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking_backtest import MomentumRankingBacktestSummary


def valid_backtest_summary() -> dict[str, object]:
    return {
        "strategy_name": "baseline_momentum",
        "strategy_version": "v1",
        "input_dataset_version": "test-v1",
        "universe_hash": "a" * 64,
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 12, 31),
        "bucket_size": 10,
        "step_observations": 20,
        "forward_horizon_observations": 20,
        "completed_ranking_date_count": 24,
        "attempted_ranking_date_count": 30,
        "positive_rank_spread_count": 15,
        "rank_spread_hit_rate": Decimal("0.625"),
        "average_rank_spread": Decimal("0.021"),
        "median_rank_spread": Decimal("0.015"),
        "generated_at": datetime(2025, 1, 1, tzinfo=UTC),
    }


def test_accepts_valid_ranking_backtest_summary() -> None:
    summary = MomentumRankingBacktestSummary.model_validate(valid_backtest_summary())

    assert summary.exchange == "NSE"
    assert summary.completed_ranking_date_count == 24
    assert summary.positive_rank_spread_count == 15
    assert summary.rank_spread_hit_rate == Decimal("0.625")


def test_rejects_positive_spread_count_above_completed_count() -> None:
    record = valid_backtest_summary()
    record["positive_rank_spread_count"] = 25

    with pytest.raises(
        ValidationError,
        match="positive_rank_spread_count cannot exceed",
    ):
        MomentumRankingBacktestSummary.model_validate(record)


def test_rejects_start_date_after_end_date() -> None:
    record = valid_backtest_summary()
    record["start_date"] = date(2025, 1, 1)

    with pytest.raises(
        ValidationError,
        match="start_date cannot be after end_date",
    ):
        MomentumRankingBacktestSummary.model_validate(record)


def test_rejects_naive_generated_at() -> None:
    record = valid_backtest_summary()
    record["generated_at"] = datetime(  # noqa: DTZ001
        2025,
        1,
        1,
        12,
    )

    with pytest.raises(
        ValidationError,
        match="generated_at must include timezone information",
    ):
        MomentumRankingBacktestSummary.model_validate(record)


def test_rejects_completed_count_above_attempted_count() -> None:
    record = valid_backtest_summary()
    record["attempted_ranking_date_count"] = 20

    with pytest.raises(
        ValidationError,
        match="completed_ranking_date_count cannot exceed",
    ):
        MomentumRankingBacktestSummary.model_validate(record)
