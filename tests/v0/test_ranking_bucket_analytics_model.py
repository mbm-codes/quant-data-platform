from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.ranking_bucket_analytics import (
    MomentumRankingBucketSummary,
)


def valid_top_bucket_summary() -> dict[str, object]:
    return {
        "ranking_run_id": "ranking-run-001",
        "input_dataset_version": "test-v1",
        "bucket": "top",
        "bucket_size": 10,
        "rank_start": 1,
        "rank_end": 10,
        "completed_count": 10,
        "average_forward_return": Decimal("0.04"),
        "median_forward_return": Decimal("0.02"),
        "positive_return_count": 7,
        "positive_return_rate": Decimal("0.70"),
        "evaluation_as_of_date": date(2024, 2, 1),
        "generated_at": datetime(2024, 2, 2, tzinfo=UTC),
    }


def test_accepts_valid_top_bucket_summary() -> None:
    summary = MomentumRankingBucketSummary.model_validate(
        valid_top_bucket_summary()
    )

    assert summary.bucket == "TOP"
    assert summary.rank_start == 1
    assert summary.rank_end == 10
    assert summary.positive_return_rate == Decimal("0.70")


def test_rejects_rank_range_in_reverse_order() -> None:
    record = valid_top_bucket_summary()
    record["rank_start"] = 10
    record["rank_end"] = 1

    with pytest.raises(ValidationError, match="rank_start cannot be after"):
        MomentumRankingBucketSummary.model_validate(record)


def test_rejects_positive_count_above_completed_count() -> None:
    record = valid_top_bucket_summary()
    record["positive_return_count"] = 11

    with pytest.raises(
        ValidationError,
        match="positive_return_count cannot exceed completed_count",
    ):
        MomentumRankingBucketSummary.model_validate(record)


def test_rejects_completed_count_above_bucket_size() -> None:
    record = valid_top_bucket_summary()
    record["completed_count"] = 11

    with pytest.raises(
        ValidationError,
        match="completed_count cannot exceed bucket_size",
    ):
        MomentumRankingBucketSummary.model_validate(record)


def test_rejects_naive_generated_at() -> None:
    record = valid_top_bucket_summary()
    record["generated_at"] = datetime(2024, 2, 2, 12)  # noqa: DTZ001

    with pytest.raises(
        ValidationError,
        match="generated_at must include timezone information",
    ):
        MomentumRankingBucketSummary.model_validate(record)